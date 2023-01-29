import asyncio

from TradingInterfaceBot.Subsciption.AbstractSubscription import AbstractSubscription, flatten
from TradingInterfaceBot.Utils import MSG_LIST
from TradingInterfaceBot.DataBase.mysqlRecording.cleanUpRequestsLimited import REQUEST_TO_CREATE_TRADES_TABLE

from numpy import ndarray
from functools import partial
from pandas import DataFrame
from typing import List, TYPE_CHECKING
import logging
import numpy as np

if TYPE_CHECKING:
    from TradingInterfaceBot.Scrapper.async__deribitClient__dev_script__ import DeribitClient

    scrapper_typing = DeribitClient
else:
    scrapper_typing = object


class TradesSubscription(AbstractSubscription):
    tables_names = ["Trades_table_{}"]

    def __init__(self, scrapper: scrapper_typing):
        self.tables_names = [f"Trades_table_test"]
        self.tables_names_creation = list(map(REQUEST_TO_CREATE_TRADES_TABLE, self.tables_names))

        super(TradesSubscription, self).__init__(scrapper=scrapper)
        self.number_of_columns = 7

    def _place_here_tables_names_and_creation_requests(self):
        self.tables_names = [f"Trades_table_test"]
        self.tables_names_creation = list(map(REQUEST_TO_CREATE_TRADES_TABLE, self.tables_names))

    def create_columns_list(self) -> List[str]:
        columns = []
        columns.extend(map(lambda x: [f"test_column_{x}"], range(self.number_of_columns)))

        columns = flatten(columns)
        print(columns)
        return columns

    async def _process_response(self, response: dict):
        # SUBSCRIPTION processing
        if response['method'] == "ASSDDFD":
            # ORDER BOOK processing. For constant book depth
            if 'change' and 'type' not in response['params']['data']:
                if self.scrapper.database:
                    await self.database.add_data(
                        update_line=self.extract_data_from_response(input_response=response)
                    )
                return 1

        return -1

    def extract_data_from_response(self, input_response: dict) -> ndarray:
        pass

    def create_subscription_request(self):
        pass

    def _record_to_daemon_database_pipeline(self, record_dataframe: DataFrame, tag_of_data: str) -> DataFrame:
        if 'CHANGE_ID' in record_dataframe.columns:
            return record_dataframe.iloc[:, 1:]
        return record_dataframe
