"""
14.01.2024

Download TimeSeries with Best Ask and Bid.

Please remind that you need to have .env file at this directory to run code
.env file have to contain DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_HOST, DB_PORT
"""
import logging
import os
from deribit_data_scrapper.Utils.AvailableInstrumentType import InstrumentType
import numpy as np
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import datetime
from time import sleep
import warnings

from tqdm import tqdm
load_dotenv(".env")


class EmptyInstrument:
    def __call__(self):
        raise TypeError("Cannot call empty instrument")


@np.vectorize
def map_instrument_type(instrument_type_int: int) -> InstrumentType | EmptyInstrument:
    if instrument_type_int == 1:
        return InstrumentType.FUTURE
    elif instrument_type_int == 2:
        return InstrumentType.OPTION
    elif instrument_type_int == 3:
        return InstrumentType.FUTURE_COMBO
    elif instrument_type_int == 4:
        return InstrumentType.OPTION_COMBO
    elif instrument_type_int == 5:
        return InstrumentType.CALL_OPTION
    elif instrument_type_int == 6:
        return InstrumentType.PUT_OPTION
    elif instrument_type_int == 7:
        return InstrumentType.ASSET
    else:
        logging.warning("Unknown Instrument")
        return EmptyInstrument()


def create_string(description: pd.DataFrame):
    if (
        description["INSTRUMENT_TYPE"] == InstrumentType.PUT_OPTION
        or description["INSTRUMENT_TYPE"] == InstrumentType.CALL_OPTION
    ):
        instrument_name = "BTC" if description["INSTRUMENT_INDEX"] == 0 else "ETH"
        strike = description["INSTRUMENT_STRIKE"]
        maturity = datetime.date.fromtimestamp(
            description["INSTRUMENT_MATURITY"]
        ).strftime("%d%b%Y")
        _option_type = (
            "Call"
            if description["INSTRUMENT_TYPE"] == InstrumentType.CALL_OPTION
            else "Put"
        )
        return f"{instrument_name}_{_option_type}-{maturity}-{strike}"
        pass
    elif description["INSTRUMENT_TYPE"] == InstrumentType.FUTURE:
        # Futures
        instrument_name = "BTC" if description["INSTRUMENT_INDEX"] == 0 else "ETH"
        strike = description["INSTRUMENT_STRIKE"]
        maturity = datetime.date.fromtimestamp(
            description["INSTRUMENT_MATURITY"]
        ).strftime("%d%b%Y")
        return f"{instrument_name}_future-{maturity}-none"
    elif description["INSTRUMENT_TYPE"] == InstrumentType.ASSET:
        # Perpetual
        instrument_name = "BTC" if description["INSTRUMENT_INDEX"] == 0 else "ETH"
        return f"{instrument_name}_perpetual-none-none"


class DataBaseConfig:
    """
    Database connection configuration
    """

    def __init__(self):
        self.USERNAME = os.getenv("DB_USERNAME", "root")
        self.PWD = os.getenv("DB_PASSWORD", "password")
        self.DATABASE_NAME = os.getenv("DB_DATABASE", "default")
        self.HOST = os.getenv("DB_HOST", "localhost")
        self.PORT = os.getenv("DB_PORT", 3306)

    def __str__(self):
        return f"{self.USERNAME=} {self.PWD=} {self.DATABASE_NAME=} {self.HOST=} {self.PORT}"


DB_CONFIG = DataBaseConfig()


def establish_mysql_connection():
    try:
        # Establish MySQL connection
        connection = mysql.connector.connect(
            host=DB_CONFIG.HOST,
            user=DB_CONFIG.USERNAME,
            password=DB_CONFIG.PWD,
            port=DB_CONFIG.PORT,
            database=DB_CONFIG.DATABASE_NAME,
        )
        # print("MySQL connection established successfully.")
        return connection
    except mysql.connector.Error as error:
        print("Error while connecting to MySQL:", error)
        return None


def close_mysql_connection(connection):
    if connection:
        connection.close()
        # print("MySQL connection closed.")


def read_data_from_mysql(query):
    connection = establish_mysql_connection()
    if connection:
        try:
            # Read data from MySQL using pandas read_sql method
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                data = pd.read_sql(query, connection)
            # print("Data read successfully from MySQL.")
            return data
        except mysql.connector.Error as error:
            print("Error while reading data from MySQL:", error)
        finally:
            close_mysql_connection(connection)


OFFSET = 400_000_000
LIMIT = 1_000_000


TOTAL = 600_000_000
total_df = pd.DataFrame()

first_run = True
file_name = 'latest_data.csv'
if __name__ == '__main__':
    for _shift in tqdm(range(OFFSET // LIMIT, TOTAL // LIMIT)):
        left_border = int(_shift * LIMIT)
        right_border = int((_shift + 1) * LIMIT) - 1
        df = read_data_from_mysql(f"SELECT * FROM TABLE_DEPTH_10 WHERE CHANGE_ID between {left_border} and {right_border}")
        time = df["TIMESTAMP_VALUE"]
        instrument_description = df[
            ["INSTRUMENT_INDEX", "INSTRUMENT_STRIKE", "INSTRUMENT_MATURITY", "INSTRUMENT_TYPE"]
        ]
        best_ask = df[["ASK_0_PRICE", "ASK_0_AMOUNT"]]
        best_bid = df[["BID_9_PRICE", "BID_9_AMOUNT"]]


        instrument_description.loc[:, "INSTRUMENT_TYPE"] = map_instrument_type(
            instrument_description["INSTRUMENT_TYPE"]
        )
        instrument_description["INSTRUMENT_NAME"] = instrument_description.apply(
            create_string, axis=1
        )


        best_ask.columns = ["ASK_PRICE", "ASK_AMOUNT"]
        best_bid.columns = ["BID_PRICE", "BID_AMOUNT"]
        # time_converted = pd.to_datetime(time * 1_000_000)
        time_converted = pd.Series([pd.Timestamp.fromtimestamp(_time // 1000) for _time in time])
        merged = pd.concat([time_converted, instrument_description, best_ask, best_bid], axis=1)
        if first_run:
            merged.to_csv(file_name, header=True, index=False)
            first_run = False
        else:
            with open(file_name, 'a') as file:
                merged.to_csv(file, header=False, index=False)

        sleep(0.5)

