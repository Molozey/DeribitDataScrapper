from downloadBest import OFFSET, TOTAL
from downloadBest import read_data_from_mysql
from downloadBest import map_instrument_type
from downloadBest import create_string
import pandas as pd
from time import sleep

# SQL_QUERY = """
#
# SET @left_border = {};
# SET @right_border = {};
# SET NOCOUNT ON;
# SELECT TIMESTAMP_VALUE INTO @left_time FROM TABLE_DEPTH_10 WHERE CHANGE_ID = @left_border;
# SET NOCOUNT ON;
# SELECT TIMESTAMP_VALUE INTO @right_time FROM TABLE_DEPTH_10 WHERE CHANGE_ID = @right_border;
# SET NOCOUNT OFF;
# SELECT * FROM Trades_table_test WHERE TIMESTAMP_VALUE between @left_time and @right_time;"""

BORDERS = """SELECT TIMESTAMP_VALUE FROM TABLE_DEPTH_10 WHERE CHANGE_ID = {}"""
REQUEST = """SELECT * FROM Trades_table_test WHERE TIMESTAMP_VALUE between {} and {};"""

SET_RIGHT_BORDER = None
SET_RIGHT_BORDER = 30_000_000 + OFFSET

first_run = True
file_name = 'trades.csv'

if __name__ == '__main__':
    left_border = read_data_from_mysql(
        BORDERS.format(OFFSET)
    )
    right_border = read_data_from_mysql(BORDERS.format(SET_RIGHT_BORDER if SET_RIGHT_BORDER else TOTAL))

    df = read_data_from_mysql(
        REQUEST.format(left_border.values[0][0], right_border.values[0][0]))
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
    if first_run:
        merged.to_csv(file_name, header=True, index=False)
        first_run = False
    else:
        with open(file_name, 'a') as file:
            merged.to_csv(file, header=False, index=False)

    sleep(0.5)
