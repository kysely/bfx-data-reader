import logwood
from logwood.handlers.stderr import ColoredStderrHandler
from argparse import ArgumentParser

# app setup --------------------------------------------------------------------
cli = ArgumentParser(description='Start up a data reader')
cli.add_argument('SYMBOLS', type=str, nargs='*',
                 help='name of the symbol/s you want to collect (btcusd, etheur, etc)')
cli.add_argument('--since', type=str, default='2019-01-01T00:00.00Z',
                 help='date since (e.g. 2019-04-29 or 2019-01-02T23:59.59Z)')
cli.add_argument('--until', type=str, default='2019-01-02T23:59.59Z',
                 help='date until (e.g. 2019-04-29 or 2019-01-02T23:59.59Z)')
cli.add_argument('--split', action='store_const', const=True,
                 default=False,
                 help='include when you want to split data info into multiples CSVs')
cli.add_argument('--debug', action='store_const', const=True,
                 default=False,
                 help='include when you want to see debug-level logs')
sysargv = cli.parse_args()

LOG_FORMAT = '{timestamp:.3f} [{level}] {message}'
logwood.basic_config(
    level=logwood.DEBUG if sysargv.debug else logwood.INFO,
    handlers=[ColoredStderrHandler(format=LOG_FORMAT)])
L = logwood.get_logger('GLOBAL')

# ------------------------------------------------------------------------------
import asyncio
import aiohttp
import csv
import os
import ujson
import time
from datetime import datetime
from dateutil import parser
from typing import List, Callable

THIS_RUN = datetime.utcnow()
BETWEEN_REQUESTS = 4

def normalize_symbol_name(sym: str) -> str:
    pair_name = sym[1:] if len(sym) == 7 and sym.startswith('t') else sym
    return f't{pair_name.upper()}'

def print_trade(trade: List) -> None:
    print(f'#{trade[0]} | {ms_to_date(trade[1])} | {trade[2]} | {trade[3]}')

# date and time related helper functions ---
def now_in_ms() -> int:
    """Returns the current UTC time in millisecond timestamp"""
    return int(round(time.time() * 1000))

def date_to_ms(date: datetime) -> int:
    """Converts Python `datetime` object to millisecond UTC timestamp"""
    return int(round(date.timestamp() * 1000))

def ms_to_date(time_in_millis: int) -> datetime:
    """Converts millisecond timestamp to Python `datetime` object in UTC"""
    time_in_s = time_in_millis/1000  # Python time works in secs, not msecs
    return datetime.utcfromtimestamp(time_in_s)

def fmt_date_long(date: datetime) -> str:
    return date.strftime("%Y-%m-%dT%H.%M.%S")

def fmt_date_short(date: datetime) -> str:
    return date.strftime("%Y-%m-%d")


# main trades processor ---
async def process_trades(trades: List, symbol: str):
    """Callback for processing valid batch of trades saved in `trades` argument.
    Each trade in the list has the following shape:
    [
        ID,
        MILLISECOND TIMESTAMP,
        AMOUNT,
        PRICE
    ]
    """
    dir_name = fmt_date_long(THIS_RUN)
    symbol = normalize_symbol_name(symbol)
    batch_name = fmt_date_short(ms_to_date(trades[0][1])) if sysargv.split \
        else 'merged'

    try:
        with open(f'./{dir_name}/trades_{symbol}_{batch_name}.csv', 'a') as csvfile:
            tradeswriter = csv.writer(csvfile, delimiter=',')
            for trade in trades:
                tradeswriter.writerow(trade)

    except FileNotFoundError:
        # dir won't exist on first run, create it and re-run processing
        os.mkdir(f'./{dir_name}')
        await process_trades(trades, symbol)


# main  ---
async def get_trades(symbol: str, start: int, end: int):
    symbol = normalize_symbol_name(symbol)
    L.info(f'-- {symbol}: Collecting since {ms_to_date(start)} until {ms_to_date(end)}')

    last_timestamp = start

    while True:
        # the outer loop will ensure the process will resume
        # even after some hard unexpecte exceptions
        try:
            async with aiohttp.ClientSession() as session:

                # we send multiple requests, always specifying trades since
                # the timestamp of the last seen trade until "now".
                # Responses are limited to 5k records, that's why need to repeat them.
                # see 'end or continue?' at the bottom of the function
                while True:
                    get_until = now_in_ms()
                    L.debug(f'{symbol}: Fetching {ms_to_date(last_timestamp+1)} to {ms_to_date(get_until)}')

                    async with session.get(
                        'https://api-pub.bitfinex.com/v2/trades/{}/hist?start={}&end={}&limit=5000&sort=1'
                            .format(normalize_symbol_name(symbol), last_timestamp+1, get_until)
                        ) as resp:

                        # when we get rejected by the server for too many requests
                        if resp.status == 429:
                            retry_after = 70 if 'Retry-After' not in resp.headers \
                                else int(resp.headers['Retry-After'])
                            L.warning(f'{symbol}: Too many requests, will resume in {retry_after}s')
                            await asyncio.sleep(retry_after+1)
                            continue

                        # unexpected case
                        if resp.status != 200:
                            L.warning(f'{symbol}: Unexpected error {await resp.text()}')
                            await asyncio.sleep(2)
                            continue

                        # we got valid trades
                        trades = await resp.json(encoding='utf-8', loads=ujson.loads)

                        if len(trades) > 0:
                            last_timestamp = trades[-1][1]

                        L.debug(f'{symbol}: Received new {len(trades)} trades')

                        # end or continue? ---
                        if get_until > end and len(trades) == 0:
                            # if we queried trades until further than
                            # `end` and nothing came in, we're done
                            break
                        elif last_timestamp < end:
                            # else, if last known trade timestamp is lower than
                            # `end`, continue and try subsequent requests
                            if len(trades) > 0:
                                await process_trades(trades, symbol)
                            await asyncio.sleep(BETWEEN_REQUESTS)
                        else:
                            # else, last known trade timestamp is bigger than `end`,
                            # we're done, filter out trades with timestamp > `end`
                            trades_until_end = list(filter(lambda t: t[1] <= end, trades))
                            L.debug(f'{symbol}: Filtered out {len(trades)-len(trades_until_end)} trades')
                            if len(trades_until_end) > 0:
                                await process_trades(trades_until_end, symbol)
                            break
            break # successful, break out of the outer while loop

        except Exception as e:
            L.error(f'{symbol}: Unexpected expection: {e}')
            L.exception(e)
            await asyncio.sleep(1) # throttle
            # the outer while loop will resume
    L.info(f'{symbol}: -- End of trades collection')


# ==============================================================================
async def main():
    if len(sysargv.SYMBOLS) == 0:
        raise RuntimeError('You have to provide at least one symbol')

    tasks = set()

    for sym in sysargv.SYMBOLS:
        tasks.add(
            asyncio.create_task(
                get_trades(
                    sym,
                    date_to_ms(parser.parse(sysargv.since)),
                    date_to_ms(parser.parse(sysargv.until))
                )
            )
        )

    await asyncio.wait(tasks, return_when='ALL_COMPLETED')

asyncio.get_event_loop().run_until_complete(main())
