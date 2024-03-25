from datetime import datetime
import numpy as np
from downloadBest import read_data_from_mysql
import pandas as pd


BASE_TOLERANCE = 100


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


def bin_search(border: pd.Timestamp) -> int:
    high = read_data_from_mysql(query=f'SELECT MAX(CHANGE_ID) FROM TABLE_DEPTH_10')["MAX(CHANGE_ID)"].iloc[0]
    # print(high)
    low = read_data_from_mysql(query=f'SELECT MIN(CHANGE_ID) FROM TABLE_DEPTH_10')["MIN(CHANGE_ID)"].iloc[0]
    # print(f"{low=} {high=}")

    max_iter = int(np.log2(high - low)) + 2

    assert get_time_by_change_id(low) < border
    assert get_time_by_change_id(high) > border


    count = 0
    while low < high - 1 or count > max_iter:
        mid = int((low + high) / 2)
        # TOLERANCE = 10_000
        # mid = get_nearest_change_id(left_border=mid-TOLERANCE, right_border=mid+TOLERANCE, left=True)
        print(mid)
        mid_val = get_time_by_change_id(mid, collect_left_item=True)
        print(mid_val)
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


if __name__ == '__main__':
    # bin_search(100_0000)

    change_id = bin_search(border=pd.Timestamp('2024-01-12 04:30:00'))
    print('_'*20)
    print(change_id)
    print(get_time_by_change_id(change_id))


