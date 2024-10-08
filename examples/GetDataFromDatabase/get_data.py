import warnings
import os
import re
import glob
import logging as log
from typing import Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv
import click

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from BestAskAndBid.downloadBest import read_data_from_mysql


log.basicConfig(
    format='%(asctime)s %(levelname)s [%(module)s:%(lineno)d] -- %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S.%03d',
    level=log.INFO)


def _get_prices_amounts(books_data: np.ndarray):
    bid_prices  = books_data[:, 0:20:2][:, ::-1]
    bid_amounts = books_data[:, 1:20:2][:, ::-1]
    ask_prices  = books_data[:, 20:40:2]
    ask_amounts = books_data[:, 21:40:2]
    return bid_prices, bid_amounts, ask_prices, ask_amounts
    

def _get_valid_mask(books_df: pd.DataFrame):
    books_data = books_df.iloc[:, 6:].to_numpy()
    prices = books_data[:, 0::2]
    amounts = books_data[:, 1::2]
    bid_prices, bid_amounts, ask_prices, ask_amounts = _get_prices_amounts(books_data)

    # Ensure that price and amount can be equal to -1 (i.e. values are missing) only simultaneously
    valid_mask1 = np.all(prices * amounts >= 0, axis=1)
    # Ensure that at least best bid or best ask level is not missing
    valid_mask2 = ((ask_prices[:, 0] != -1) & (ask_amounts[:, 0] != -1)) | \
                  ((bid_prices[:, 0] != -1) & (bid_amounts[:, 0] != -1))
    valid_mask = valid_mask1 & valid_mask2

    return valid_mask


def process_books(books: pd.DataFrame):
    """Processes the original order book table."""

    valid_mask = _get_valid_mask(books)
    books = books.drop(index=np.nonzero(~valid_mask)[0])
    books_data = books.iloc[:, 6:].to_numpy()
    books_data = np.where(books_data == -1, 0, books_data)

    bid_prices, bid_amounts, ask_prices, ask_amounts = _get_prices_amounts(books_data)

    books['bid_amount_total'] = bid_amounts.sum(axis=1)
    books['ask_amount_total'] = ask_amounts.sum(axis=1)

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        bid_vwap = np.sum(bid_prices * bid_amounts, axis=1) / books['bid_amount_total'].to_numpy()
        ask_vwap = np.sum(ask_prices * ask_amounts, axis=1) / books['ask_amount_total'].to_numpy()

    # valid when price and amount can be -1 only simultaneously (we ensured this above)
    books['bid_vwap'] = np.nan_to_num(bid_vwap, copy=False, nan=-1.0)
    books['ask_vwap'] = np.nan_to_num(ask_vwap, copy=False, nan=-1.0)

    books = books[
            ['INSTRUMENT_INDEX', 'INSTRUMENT_STRIKE', 'INSTRUMENT_MATURITY',
            'INSTRUMENT_TYPE', 'TIMESTAMP_VALUE', 'BID_9_PRICE', 'ASK_0_PRICE',
            'bid_amount_total', 'ask_amount_total', 'bid_vwap', 'ask_vwap']
        ] \
        .rename(columns={
            'INSTRUMENT_INDEX':    'instrument_id',
            'INSTRUMENT_STRIKE':   'strike',
            'INSTRUMENT_MATURITY': 'maturity',
            'INSTRUMENT_TYPE':     'instrument_type',
            'TIMESTAMP_VALUE':     'timestamp',
            'BID_9_PRICE':         'best_bid_price',
            'ASK_0_PRICE':         'best_ask_price',
        })
    books.sort_values('timestamp', ignore_index=True, inplace=True)
    return books


def initialize_data_dir(data_dir, glob_str):
    last_idx = None
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    else:
        files = [f for f in glob.glob(data_dir + '/' + glob_str) if os.path.isfile(f)]

        idxs = []
        for file in files:
            # find first digit in file name
            s = os.path.basename(file).split('.')[0]
            if (match := re.search(r"\d", s)):
                idxs.append(int(s[match.start():]))

        if len(idxs) != 0:
            idxs.sort()
            last_idx = idxs[-1]
    return last_idx


def get_change_id_range(table_name) -> Tuple[int, int]:
    query = f"""
    SELECT MIN(CHANGE_ID) AS min_change_id,
           MAX(CHANGE_ID) AS max_change_id
    FROM {table_name}
    """
    result = read_data_from_mysql(query)
    assert len(result) == 1
    min_change_id = result.loc[0, 'min_change_id']
    max_change_id = result.loc[0, 'max_change_id']
    return min_change_id, max_change_id


def get_timestamp_range(table_name) -> Tuple[pd.Timestamp, pd.Timestamp]:
    query = f"""
    SELECT MIN(TIMESTAMP_VALUE) AS min_timestamp,
           MAX(TIMESTAMP_VALUE) AS max_timestamp
    FROM {table_name}
    """
    result = read_data_from_mysql(query)
    assert len(result) == 1
    min_timestamp = pd.to_datetime(result.loc[0, 'min_timestamp'], unit='ms')
    max_timestamp = pd.to_datetime(result.loc[0, 'max_timestamp'], unit='ms')
    return min_timestamp, max_timestamp


@click.command(context_settings=dict(max_content_width=90))
@click.option('--start-idx', type=int, default=1, show_default=True,
              help='Start index of the data to download')
@click.option('--end-idx', type=int, default=2_148_000_000, show_default=True,
              help='End index of the data to download')
@click.option('--batch-size', type=int, default=4_000_000, show_default=True,
              help='Batch size')
@click.option('--fetch-table-info', is_flag=True, help='Fetch database table info. May be time-consuming.')
@click.argument('data-type', type=click.Choice(['books_raw', 'books_processed', 'trades']))
@click.argument('save-dir', type=str)
def main(start_idx, end_idx, batch_size, fetch_table_info, data_type, save_dir):
    """Download selected type of data from the database and save it into SAVE_DIR.
    
    Available data types: `books_raw`, `books_processed`, `trades`.
    """
    load_dotenv(".env")
    log.info(f'Data type: `{data_type}`')
    log.info(f'Will save data into directory: `{save_dir}`')
    log.info(f'Start index: {start_idx}. End index: {end_idx}. Batch size: {batch_size}')

    if data_type in ('books_raw', 'books_processed'):
        table_name = 'TABLE_DEPTH_10'
        glob_str = 'books[0-9]*.hdf'
        data_file_fmt = save_dir + '/books{:0004d}.hdf'
    elif data_type == 'trades':
        table_name = 'Trades_table_test'
        glob_str = 'trades[0-9]*.hdf'
        data_file_fmt = save_dir + '/trades{:0004d}.hdf'

    if (batch_num := initialize_data_dir(save_dir, glob_str)) is not None:
        start_idx = start_idx + (batch_num - 1) * batch_size
        log.info(f'Existing data directory found, continuing from batch {batch_num} (index={start_idx})')
    else:
        batch_num = 1
        start_idx = start_idx

    log.info(f'Database table: `{table_name}`')
    if fetch_table_info:
        log.info(f'Fetching table info...')
        min_change_id, max_change_id = get_change_id_range(table_name)
        min_ts, max_ts = get_timestamp_range(table_name)
        log.info(f'Table CHANGE_ID range: {min_change_id} - {max_change_id}')
        log.info(f'Table TIMESTAMP range: {min_ts} - {max_ts}')

    for _shift in range(start_idx // batch_size, end_idx // batch_size):
        left_border = int(_shift * batch_size)
        right_border = int((_shift + 1) * batch_size) - 1
        log.info('')
        log.info(f'Batch: {batch_num}, CHANGE_ID range: {left_border} - {right_border}. Downloading data...')

        df = read_data_from_mysql(f"""
            SELECT * FROM {table_name}
            WHERE CHANGE_ID between {left_border} and {right_border}
        """)
        if len(df) == 0:
            log.info('No more data to download, stopping')
            break
        
        min_ts_obtained = pd.to_datetime(df["TIMESTAMP_VALUE"].min(), unit="ms")
        max_ts_obtained = pd.to_datetime(df["TIMESTAMP_VALUE"].max(), unit="ms")
        log.info(f'Done. CHANGE_ID range: {df["CHANGE_ID"].min()} - {df["CHANGE_ID"].max()}')
        log.info(f'TIMESTAMP range: {min_ts_obtained} - {max_ts_obtained}.')
        
        if data_type == 'books_processed':
            log.info(f'Processing books...')
            df = process_books(df)
        
        file_name = data_file_fmt.format(batch_num)
        df.to_hdf(file_name, key=data_type, complevel=3)
        log.info(f'Saved to file `{os.path.basename(file_name)}`')
        batch_num += 1

    log.info('')
    log.info(f'All done!')

if __name__ == '__main__':
    main()