"""
18.02.2024

Processor for saving scrapper rcp information

Please remind that you need to have .env file at this directory to run code
.env file have to contain DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_HOST, DB_PORT
"""
import time
from typing import Optional

from dotenv import load_dotenv
from pandas import Timestamp

from examples.BestAskAndBid.downloadBest import DataBaseConfig
from examples.BestAskAndBid.downloadBest import read_data_from_mysql

load_dotenv(".env")
DB_CONFIG = DataBaseConfig()

UPDATE_TIME_IN_SEC = 10
ORDERBOOK_FILE = "orderBookShape.txt"
TRADES_FILE = "tradesShape.txt"

DEPTH = 10

if __name__ == "__main__":
    prev_book: Optional[int] = None
    prev_trade: Optional[int] = None

    while True:
        orderbook_shape = read_data_from_mysql(
            query="""SELECT MAX(CHANGE_ID) FROM TABLE_DEPTH_{}""".format(DEPTH)
        )
        orderbook_shape = orderbook_shape.iloc[0]["MAX(CHANGE_ID)"]
        with open(ORDERBOOK_FILE, "a") as orderbook_file:
            orderbook_file.write(
                f"{Timestamp.utcnow().replace(tzinfo=None)},{orderbook_shape}\n"
            )

        trades_shape = read_data_from_mysql(
            query="""SELECT MAX(CHANGE_ID) FROM Trades_table_test"""
        )
        trades_shape = trades_shape.iloc[0]["MAX(CHANGE_ID)"]

        with open(TRADES_FILE, "a") as trades_file:
            trades_file.write(
                f"{Timestamp.utcnow().replace(tzinfo=None)},{trades_shape}\n"
            )

        if prev_trade and prev_book:
            with open("RPC_" + ORDERBOOK_FILE, "a") as rpc_orderbook_file:
                rpc_orderbook_file.write(
                    f"{Timestamp.utcnow().replace(tzinfo=None)},{(orderbook_shape - prev_book) / UPDATE_TIME_IN_SEC},{UPDATE_TIME_IN_SEC}\n"
                )

            with open("RPC_" + TRADES_FILE, "a") as rpc_trades_file:
                rpc_trades_file.write(
                    f"{Timestamp.utcnow().replace(tzinfo=None)},{(trades_shape - prev_trade) / UPDATE_TIME_IN_SEC},{UPDATE_TIME_IN_SEC}\n"
                )

        time.sleep(UPDATE_TIME_IN_SEC)
        prev_trade = trades_shape
        prev_book = orderbook_shape
