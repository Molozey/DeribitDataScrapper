from enum import Enum
import pandas as pd
import numpy as np
from tqdm.auto import tqdm


def resample_data(freq, n_files, file_path_format, file_path_resampled):
    # here all timestamps are in ms
    freq_ms = round(pd.to_timedelta(freq).total_seconds()*1000)
    samples = []
    sample = {}
    sample_idx = 1

    for table_idx in tqdm(range(1, n_files+1), ncols=70):
        books = pd.read_hdf(file_path_format.format(table_idx))

        # TODO: pass to function names of the columns
        instr_id = books['instrument_id'].to_numpy()
        strike = books['strike'].to_numpy()
        maturity = books['maturity'].to_numpy() * 1000  # convert to ms
        instr_type = books['instrument_type'].to_numpy()
        ts = books['timestamp'].to_numpy() # in ms

        if table_idx == 1:
            start_ts = ts[0]
            end_ts = start_ts + freq_ms

        for row in range(len(ts)):
            if ts[row] <= end_ts:
                key = (instr_id[row], strike[row], maturity[row], instr_type[row])
                if maturity[row] < 0 or maturity[row] > end_ts:
                    sample[key] = (sample_idx, table_idx, row)
            else:
                samples += list(sample.values())
                start_ts = ts[row]
                end_ts = start_ts + freq_ms
                sample = {} 
                sample_idx += 1

    samples_data = np.array(samples, dtype=int)
    # sort by table_idx so that we can iterate tables sequentially
    samples_data = samples_data[samples_data[:, 1].argsort(kind='stable')]

    for table_idx in tqdm(range(1, n_files+1), ncols=70):
        books = pd.read_hdf(file_path_format.format(table_idx))

        if table_idx == 1:
            books_resampled = books.drop(books.index)
            books_resampled.insert(0, 'sample_idx', -1)

        curr_table_mask = (samples_data[:, 1] == table_idx)
        rows = samples_data[curr_table_mask][:, 2]
        books_curr_table = books.iloc[rows].copy()
        books_curr_table['sample_idx'] = samples_data[curr_table_mask][:, 0]
        books_resampled = pd.concat((books_resampled, books_curr_table), ignore_index=True)

    books_resampled.sort_values('sample_idx', kind='stable', ignore_index=True, inplace=True)
    books_resampled.to_hdf(file_path_resampled, key='books_processed', complevel=3)


if __name__ == '__main__':

    FREQ = '12h'
    N_FILES = 2148
    FILE_PATH = 'DATA/books_processed/books{:0004d}.hdf'
    FILE_PATH_RESAMPLED = f'DATA/books_resampled_{FREQ.replace(' ', '')}.hdf'

    resample_data(FREQ, N_FILES, FILE_PATH, FILE_PATH_RESAMPLED)
