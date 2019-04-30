## Bitfinex Data Reader

```sh
# first install all the packages
pip install -r requirements.txt

# usage: run.py [-h] [--since SINCE] [--until UNTIL] [--split] [--debug]
#               [SYMBOLS [SYMBOLS ...]]

# Example:
python3 run.py --debug --since 2019-04-29 --until 2019-04-30T00:00.00Z btcusd xrpbtc
```

### File Output
Each run of the process will create its own timestamped directory
to avoid record duplicates within the same files across multiple runs.

By default, trades data will be saved inside that folder.

If you need to split the data into multiple files, batching can be turned on
by passing `--split` flag. That way, trades data will be saved in batches 
across multiple CSV files. **IMPORTANT: The batch (file) timestamp is derived
from the timestamp of the first record in the batch and thus is intended for
basic orientation.** The file can contain records from an upcoming
day and the file with previous-day timestamp can contain this day's data.
