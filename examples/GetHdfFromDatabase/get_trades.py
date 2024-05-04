import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from utils import check_data_dir

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from BestAskAndBid.downloadBest import read_data_from_mysql


if __name__ == '__main__':

    load_dotenv(".env")

    START_IDX  = 1
    END_IDX    = 50_000_000
    BATCH_SIZE = 5_000_000

    DATA_DIR = 'DATA/trades'
    DATA_FILE = DATA_DIR + '/trades{:003d}.hdf'
    GLOB_STR = 'trades[0-9]*.hdf'

    if (batch_num := check_data_dir(DATA_DIR, GLOB_STR)) is not None:
        start_idx = START_IDX + (batch_num - 1) * BATCH_SIZE
        print(f'Existing data directory found, continuing from batch {batch_num}(index={start_idx})')
    else:
        batch_num = 1
        start_idx = START_IDX

    for _shift in tqdm(range(start_idx // BATCH_SIZE, END_IDX // BATCH_SIZE), ncols=70):
        left_border = int(_shift * BATCH_SIZE)
        right_border = int((_shift + 1) * BATCH_SIZE) - 1
        df = read_data_from_mysql(f"SELECT * FROM Trades_table_test WHERE CHANGE_ID between {left_border} and {right_border}")
        df.to_hdf(DATA_FILE.format(batch_num), key='trades', complevel=3)
        batch_num += 1
