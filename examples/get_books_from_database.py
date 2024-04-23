import os
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from BestAskAndBid.downloadBest import read_data_from_mysql, map_instrument_type, create_string

load_dotenv(".env")

DATA_DIR = 'DATA/books'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
else:
    raise RuntimeError(f'Data path already exists: {DATA_DIR}')

DATA_FILE = DATA_DIR + '/books{:0004d}.parquet'

START_IDX = 400_000_000
END_IDX   = 1_000_000_000
BATCH_SIZE = 1_000_000

if __name__ == '__main__':
    n = 1
    for _shift in tqdm(range(START_IDX // BATCH_SIZE, END_IDX // BATCH_SIZE)):
        left_border = int(_shift * BATCH_SIZE)
        right_border = int((_shift + 1) * BATCH_SIZE)
        df = read_data_from_mysql(f"SELECT * FROM TABLE_DEPTH_10 WHERE CHANGE_ID between {left_border} and {right_border}")
        df.to_parquet(DATA_FILE.format(n))
        n += 1
