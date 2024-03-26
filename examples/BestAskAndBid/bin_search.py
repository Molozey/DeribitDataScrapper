from typing import Union
import numpy as np
import pandas as pd
from downloadBest import read_data_from_mysql, map_instrument_type, create_string

BASE_TOLERANCE = 100


def get_nearest_change_id(left_border: int, right_border: int, left: bool = False):
    if left:
        query = f"""
        SELECT MIN(CHANGE_ID)
        FROM TABLE_DEPTH_10
        WHERE CHANGE_ID between {left_border} and {right_border}
        """
        res = read_data_from_mysql(query=query)["MIN(CHANGE_ID)"]
    else:
        query = f"""
        SELECT MAX(CHANGE_ID)
        FROM TABLE_DEPTH_10
        WHERE CHANGE_ID between {left_border} and {right_border}
        """
        res = read_data_from_mysql(query=query)["MAX(CHANGE_ID)"]
    if res.empty:
        raise KeyError("Probably big gap in data {} {}".format(left_border, right_border))
    return res.iloc[0]


def get_time_by_change_id(changeid: int, collect_left_item: bool = False) -> pd.Timestamp:
    line = read_data_from_mysql(query=f'SELECT * FROM TABLE_DEPTH_10 WHERE CHANGE_ID={changeid}')
    idx = None
    TOLERANCE = BASE_TOLERANCE

    while line.empty or not idx:
        idx = get_nearest_change_id(changeid - TOLERANCE, changeid + TOLERANCE, left=collect_left_item)
        if idx:
            line = read_data_from_mysql(
                query='SELECT * FROM TABLE_DEPTH_10 WHERE CHANGE_ID={}'.format(
                    get_nearest_change_id(changeid - TOLERANCE, changeid + TOLERANCE, left=collect_left_item)
                )
            )
        TOLERANCE = TOLERANCE * 2
        print(f"NEW TOLERANCE is {TOLERANCE}")

    line = line.iloc[0]
    time = pd.Timestamp.fromtimestamp(line["TIMESTAMP_VALUE"] // 1000)
    return time


def bin_search(border: pd.Timestamp) -> int:
    high = read_data_from_mysql(query=f'SELECT MAX(CHANGE_ID) FROM TABLE_DEPTH_10')["MAX(CHANGE_ID)"].iloc[0]
    low = read_data_from_mysql(query=f'SELECT MIN(CHANGE_ID) FROM TABLE_DEPTH_10')["MIN(CHANGE_ID)"].iloc[0]
    print(f"{low=} {high=}")

    max_iter = int(np.log2(high - low)) + 2
    assert get_time_by_change_id(low) < border
    assert get_time_by_change_id(high) > border


    count = 0
    while low < high - 1 or count > max_iter:
        mid = int((low + high) / 2)
        # TOLERANCE = 10_000
        # mid = get_nearest_change_id(left_border=mid-TOLERANCE, right_border=mid+TOLERANCE, left=True)
        # print(mid)
        mid_val = get_time_by_change_id(mid, collect_left_item=True)
        # print(mid_val)
        count += 1

        if mid_val < border:
            low = mid
        elif mid_val > border:
            high = mid
        else:
            return mid

    if count <= max_iter:
        low_delta = abs(border - get_time_by_change_id(low, collect_left_item=False))
        high_delta = abs(border - get_time_by_change_id(high, collect_left_item=True))
        if high_delta < low_delta:
            return high
        else:
            return low


def get_change_id_borders(left_timestamp: pd.Timestamp, right_timestamp: pd.Timestamp) -> tuple:
    assert left_timestamp < right_timestamp
    left_id = bin_search(left_timestamp)
    right_id = bin_search(right_timestamp)
    assert left_id < right_id
    return left_id, right_id


# TODO: What to do with best bid and ask??
# TODO: make batching

def get_trades_by_time(start_time: pd.Timestamp, end_time: pd.Timestamp, agg_ohlc: int=None) -> Union[pd.DataFrame, dict]:
    left_border, right_border = get_change_id_borders(start_time, end_time)
    print('Found CHANGE ID for time interval, getting data')

    # TODO: change trades_table_test to table
    REQUEST = """SELECT * FROM Trades_table_test WHERE CHANGE_ID between {} and {};"""

    df = read_data_from_mysql(
        REQUEST.format(left_border, right_border))

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

    # TODO: add aggregation and resampling
    # TODO: add volume bars option
    if agg_ohlc:

        return
    else:
        return merged


if __name__ == '__main__':
    data = get_trades_by_time(pd.Timestamp('2023-12-01 00:00:00'), pd.Timestamp('2023-12-01 00:05:00'))
    print(data.shape)
    print(data.columns)
    print(data.head(100))
    # bin_search(100_0000)

    # change_id = bin_search(border=pd.Timestamp('2024-01-12 04:30:00'))
    # print('_'*20)
    # print(change_id)
    # print(get_time_by_change_id(change_id))


