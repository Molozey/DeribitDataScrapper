import os
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import numpy as np
import warnings

from BestAskAndBid.downloadBest import read_data_from_mysql, map_instrument_type, create_string


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

    with warnings.catch_warnings(action='ignore'):
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


if __name__ == '__main__':
    load_dotenv(".env")

    DATA_DIR = 'DATA/books_processed'
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    else:
        raise RuntimeError(f'Data path already exists, won\'t overwrite: {DATA_DIR}')

    DATA_FILE = DATA_DIR + '/books{:0004d}.hdf'

    START_IDX  = 1
    END_IDX    = 2_148_000_000
    BATCH_SIZE = 1_000_000

    n = 1
    for _shift in tqdm(range(START_IDX // BATCH_SIZE, END_IDX // BATCH_SIZE), ncols=70):
        left_border = int(_shift * BATCH_SIZE)
        right_border = int((_shift + 1) * BATCH_SIZE)
        books = read_data_from_mysql(f"SELECT * FROM TABLE_DEPTH_10 WHERE CHANGE_ID between {left_border} and {right_border}")
        books_processed = process_books(books)
        books_processed.to_hdf(DATA_FILE.format(n), key='books_processed', complevel=3)
        n += 1
