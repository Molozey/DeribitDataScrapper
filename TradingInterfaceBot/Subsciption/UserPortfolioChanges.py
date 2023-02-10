from TradingInterfaceBot.Subsciption.AbstractSubscription import AbstractSubscription, flatten, RequestTypo
from TradingInterfaceBot.Utils import *

from numpy import ndarray
from pandas import DataFrame
from typing import List, TYPE_CHECKING
import numpy as np

if TYPE_CHECKING:
    from TradingInterfaceBot.Scrapper.TradingInterface import DeribitClient

    scrapper_typing = DeribitClient
else:
    scrapper_typing = object


class UserPortfolioSubscription(AbstractSubscription):
    tables_names = ["User_Portfolio_{}"]

    def __init__(self, scrapper: scrapper_typing):
        self.tables_names = [f"User_Portfolio_test"]
        self.tables_names_creation = list(map(REQUEST_TO_CREATE_TRADES_TABLE, self.tables_names))

        super(UserPortfolioSubscription, self).__init__(scrapper=scrapper, request_typo=RequestTypo.PRIVATE)
        self.number_of_columns = 7
        self.instrument_name_instrument_id_map = self.scrapper.instrument_name_instrument_id_map

    def _place_here_tables_names_and_creation_requests(self):
        self.tables_names = [f"User_Portfolio_test"]
        self.tables_names_creation = list(map(REQUEST_TO_CREATE_TRADES_TABLE, self.tables_names))

    def create_columns_list(self) -> List[str]:
        columns = ["CHANGE_ID", "TIMESTAMP_VALUE", "TRADE_ID", "PRICE", "NAME_INSTRUMENT", "DIRECTION", "AMOUNT"]
        columns = flatten(columns)
        return columns

    async def _process_response(self, response: dict):
        # SUBSCRIPTION processing
        if response['method'] == "subscription":
            # ORDER BOOK processing. For constant book depth
            if 'params' in response:
                if 'channel' in response['params']:
                    if 'trades' in response['params']['channel']:
                        if self.scrapper.connected_strategy is not None:
                            await self.scrapper.connected_strategy.on_trade_update(callback=response)

                        if self.database:
                            await self.database.add_data(
                                update_line=self.extract_data_from_response(input_response=response)
                            )
                            return 1

    def extract_data_from_response(self, input_response: dict) -> ndarray:
        _full_ndarray = []
        for data_object in input_response['params']['data']:
            _change_id = 666

            _timestamp = data_object['timestamp']
            _instrument_name = self.instrument_name_instrument_id_map[
                data_object['instrument_name']]
            _trade_id = data_object['trade_id']
            _price = data_object["price"]
            _direction = 1 if data_object["direction"] == "buy" else -1
            _amount = data_object["amount"]
            _full_ndarray.append(
                [_change_id, _timestamp, _trade_id, _price, _instrument_name, _direction, _amount]
            )
        return np.array(_full_ndarray)

    def _create_subscription_request(self):
        self._user_portfolio_changes_subscription_request()

    def _record_to_daemon_database_pipeline(self, record_dataframe: DataFrame, tag_of_data: str) -> DataFrame:
        if 'CHANGE_ID' in record_dataframe.columns:
            return record_dataframe.iloc[:, 1:]
        return record_dataframe

    def _user_portfolio_changes_subscription_request(self):
        subscription_message = \
            MSG_LIST.get_user_portfolio_request(
                currency=self.scrapper.client_currency,
            )
        self.scrapper.send_new_request(request=subscription_message)

