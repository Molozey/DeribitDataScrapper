from typing import Union, Optional
import numpy as np
import pandas as pd
from tqdm import tqdm

from downloadBest import read_data_from_mysql, map_instrument_type, create_string

BASE_TOLERANCE = 100


def get_table(table: str) -> str:
    if table == 'trades':
        return 'Trades_table_test'
    elif table == 'book':
        return 'TABLE_DEPTH_10'
    else:
        raise NameError(f"No table with name {table}")


def get_nearest_change_id(left_border: int,
                          right_border: int,
                          left: bool = False,
                          table: str = 'trades') -> int:
    if left:
        query = f"""
        SELECT MIN(CHANGE_ID)
        FROM {get_table(table)}
        WHERE CHANGE_ID between {left_border} and {right_border}
        """
        res = read_data_from_mysql(query=query)["MIN(CHANGE_ID)"]
    else:
        query = f"""
        SELECT MAX(CHANGE_ID)
        FROM {get_table(table)}
        WHERE CHANGE_ID between {left_border} and {right_border}
        """
        res = read_data_from_mysql(query=query)["MAX(CHANGE_ID)"]
    if res.empty:
        raise KeyError("Probably big gap in data {} {}".format(left_border, right_border))
    return res.iloc[0]


def get_time_by_change_id(changeid: int,
                          collect_left_item: bool = False,
                          table: str = 'trades') -> pd.Timestamp:

    line = read_data_from_mysql(query=f'SELECT * FROM {get_table(table)} WHERE CHANGE_ID={changeid}')
    idx = None
    TOLERANCE = BASE_TOLERANCE

    while line.empty or not idx:
        idx = get_nearest_change_id(changeid - TOLERANCE, changeid + TOLERANCE, left=collect_left_item, table=table)
        if idx:
            line = read_data_from_mysql(
                query='SELECT * FROM {} WHERE CHANGE_ID={}'.format(get_table(table),
                    get_nearest_change_id(changeid - TOLERANCE, changeid + TOLERANCE, left=collect_left_item, table=table)
                )
            )
        TOLERANCE = TOLERANCE * 2

    line = line.iloc[0]
    time = pd.Timestamp.fromtimestamp(line["TIMESTAMP_VALUE"] // 1000)
    return time


def bin_search(border: pd.Timestamp,
               table: str = 'trades') -> int:

    high = read_data_from_mysql(query=f'SELECT MAX(CHANGE_ID) FROM {get_table(table)}')["MAX(CHANGE_ID)"].iloc[0]
    low = read_data_from_mysql(query=f'SELECT MIN(CHANGE_ID) FROM {get_table(table)}')["MIN(CHANGE_ID)"].iloc[0]
    print(f"{low=} {high=}")

    max_iter = int(np.log2(high - low)) + 2
    assert get_time_by_change_id(low, table=table) <= border
    assert get_time_by_change_id(high, table=table) >= border

    count = 0
    while low < high - 1 or count > max_iter:
        mid = int((low + high) / 2)
        mid_val = get_time_by_change_id(mid, collect_left_item=True, table=table)
        count += 1
        if count - (count // 10) * 10 == 1:
            print(f'iterations: {count}/{max_iter}')

        if mid_val < border:
            low = mid
        elif mid_val > border:
            high = mid
        else:
            return mid

    if count <= max_iter:
        low_delta = abs(border - get_time_by_change_id(low, collect_left_item=False, table=table))
        high_delta = abs(border - get_time_by_change_id(high, collect_left_item=True, table=table))
        if high_delta < low_delta:
            return high
        else:
            return low


def get_change_id_borders(left_timestamp: pd.Timestamp,
                          right_timestamp: pd.Timestamp,
                          table: str = 'trades') -> tuple:

    assert left_timestamp < right_timestamp
    print('Searching for left border CHANGE_ID')
    left_id = bin_search(left_timestamp, table=table)

    print('Searching for right border CHANGE_ID')
    right_id = bin_search(right_timestamp, table=table)
    assert left_id < right_id
    return left_id, right_id


# TODO: make batching
def get_trades_by_time(start_time: pd.Timestamp,
                       end_time: pd.Timestamp,
                       table: str = 'trades') -> Union[pd.DataFrame, dict]:

    left_border, right_border = get_change_id_borders(start_time, end_time, table)
    print('Found CHANGE ID for time interval, getting data')

    REQUEST = """SELECT * FROM {} WHERE CHANGE_ID between {} and {};"""

    df = read_data_from_mysql(
        REQUEST.format(get_table(table), left_border, right_border))

    time = df["TIMESTAMP_VALUE"]
    instrument_description = df[
        ["INSTRUMENT_INDEX", "INSTRUMENT_STRIKE", "INSTRUMENT_MATURITY", "INSTRUMENT_TYPE"]
    ]

    trade_info = df[["DIRECTION", "AMOUNT", "PRICE"]]

    instrument_description.loc[:, "INSTRUMENT_TYPE"] = map_instrument_type(
        instrument_description["INSTRUMENT_TYPE"]
    )
    instrument_description["INSTRUMENT_NAME"] = instrument_description.apply(
        create_string, axis=1
    )

    # time_converted = pd.to_datetime(time * 1_000_000)
    time_converted = pd.Series([pd.Timestamp.fromtimestamp(_time // 1000) for _time in time])
    merged = pd.concat([time_converted, instrument_description, trade_info], axis=1)

    # if first_run:
    #     merged.to_csv(file_name, header=True, index=False)
    #     first_run = False
    # else:
    #     with open(file_name, 'a') as file:
    #         merged.to_csv(file, header=False, index=False)
    return merged


def agg_trades(data):
    data = data.resample('60s', on=0).agg({'PRICE': 'ohlc', 'AMOUNT': 'sum'})
    data.columns = pd.Index(['open', 'high', 'low', 'close', 'volume'])
    return data


def process_trades(data: pd.DataFrame,
                   agg_ohlc: int = None,
                   save_name: str = None) -> Optional[dict]:
    # splitting
    eth_data = data.loc[data['INSTRUMENT_INDEX'] == 1]
    btc_data = data.loc[data['INSTRUMENT_INDEX'] == 0]

    underlying_dict = {}
    for name, data in zip(['ETH', 'BTC'], [eth_data, btc_data]):
        types = pd.unique(data['INSTRUMENT_TYPE'])
        types_dict = {}
        for T in tqdm(types):
            types_data = data.loc[data['INSTRUMENT_TYPE'] == T]
            instruments = pd.unique(types_data.INSTRUMENT_NAME)
            data_dict = {}
            for el in instruments:
                temp = types_data.loc[types_data['INSTRUMENT_NAME'] == el]
                data_dict[el] = {'strike': temp['INSTRUMENT_STRIKE'].values[0] if len(temp['INSTRUMENT_STRIKE'].values) != 0 else '?',
                                 'maturity': temp['INSTRUMENT_MATURITY'].values[0] if len(temp['INSTRUMENT_MATURITY'].values) != 0 else '?',
                                 'data': temp[[0, 'DIRECTION', 'AMOUNT', 'PRICE']] if not agg_ohlc else agg_trades(temp)}
            types_dict[T] = data_dict
        underlying_dict[name] = types_dict

    if save_name:
        import os
        if not os.path.exists(save_name):
            os.makedirs(save_name)
        else:
            print(f'path {save_name} already exists')
        for underlying in ('ETH', 'BTC'):
            os.makedirs(f'{save_name}/{underlying}')
            types = list(underlying_dict[underlying].keys())
            for T in types:
                os.makedirs(f'{save_name}/{underlying}/{T}')
                instruments = list(underlying_dict[underlying][T].keys())
                for inst in instruments:
                    temp_path = f'{save_name}/{underlying}/{T}/{inst}'
                    os.makedirs(temp_path)
                    data_dict = underlying_dict[underlying][T][inst]
                    filename = f"Strike{data_dict['strike']}_Maturity_{data_dict['maturity']}"
                    data_dict['data'].to_csv(temp_path+'/'+filename+'.csv')
    else:
        return underlying_dict


if __name__ == '__main__':
    data = get_trades_by_time(pd.Timestamp('2023-12-01 00:00:00'), pd.Timestamp('2023-12-01 00:05:00'), table='trades')
    print(data.shape)
    print(data.columns)
    print(data.head(100))


