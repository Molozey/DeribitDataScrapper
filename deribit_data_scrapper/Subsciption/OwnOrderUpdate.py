from typing import List
from typing import TYPE_CHECKING

import numpy as np
from numpy import ndarray
from pandas import DataFrame

from deribit_data_scrapper.Subsciption.AbstractSubscription import AbstractSubscription
from deribit_data_scrapper.Subsciption.AbstractSubscription import flatten
from deribit_data_scrapper.Subsciption.AbstractSubscription import RequestTypo
from deribit_data_scrapper.Utils import *

if TYPE_CHECKING:
    from deribit_data_scrapper.Scrapper.TradingInterface import DeribitClient

    scrapper_typing = DeribitClient
else:
    scrapper_typing = object


class OwnOrdersSubscription(AbstractSubscription):
    """
    Class for user orders subscription. It is used to get information about user orders.
    """

    tables_names = ["User_orders_test{}"]

    def __init__(self, scrapper: scrapper_typing):
        self.tables_names = [f"User_orders_test"]
        self.tables_names_creation = list(
            map(REQUEST_TO_CREATE_OWN_ORDERS_TABLE, self.tables_names)
        )

        super(OwnOrdersSubscription, self).__init__(
            scrapper=scrapper, request_typo=RequestTypo.PRIVATE
        )
        self.number_of_columns = 16
        self.instrument_name_instrument_id_map = (
            self.scrapper.instrument_name_instrument_id_map
        )

    def _place_here_tables_names_and_creation_requests(self):
        self.tables_names = [f"User_orders_test"]
        self.tables_names_creation = list(
            map(REQUEST_TO_CREATE_OWN_ORDERS_TABLE, self.tables_names)
        )

    def create_columns_list(self) -> List[str]:
        columns = [
            "CHANGE_ID",
            "CREATION_TIMESTAMP",
            "LAST_UPDATE_TIMESTAMP",
            "INSTRUMENT_INDEX",
            "INSTRUMENT_STRIKE",
            "INSTRUMENT_MATURITY",
            "INSTRUMENT_TYPE",
            "ORDER_TYPE",
            "ORDER_STATE",
            "ORDER_ID",
            "FILLED_AMOUNT",
            "COMMISSION",
            "AVERAGE_PRICE",
            "PRICE",
            "DIRECTION",
            "AMOUNT",
        ]
        columns = flatten(columns)
        return columns

    async def _process_response(self, response: dict):
        if "result" in response:
            if "order" in response["result"]:
                if self.scrapper.order_manager is not None:
                    await self.scrapper.order_manager.process_order_callback(
                        callback=response
                    )

        # SUBSCRIPTION processing
        if response["method"] == "subscription":
            # ORDER BOOK processing. For constant book depth
            if "params" in response:
                if "channel" in response["params"]:
                    if "orders" in response["params"]["channel"]:
                        if self.scrapper.order_manager is not None:
                            await self.scrapper.order_manager.process_order_callback(
                                callback=response
                            )

                        if self.database:
                            await self.database.add_data(
                                update_line=self.extract_data_from_response(
                                    input_response=response
                                )
                            )
                            return 1

    def extract_data_from_response(self, input_response: dict) -> ndarray:
        _full_ndarray = []
        data_object = input_response["params"]["data"]

        _change_id = 666
        _creation_time = data_object["creation_timestamp"]
        _last_update = data_object["last_update_timestamp"]
        (
            _ins_idx,
            _instrument_strike,
            _instrument_maturity,
            _instrument_type,
        ) = self.instrument_name_instrument_id_map[
            data_object["instrument_name"]
        ].get_fields()
        _order_type = data_object["order_type"]
        _order_state = data_object["order_state"]
        _order_id = data_object["order_id"]
        _filled_amount = data_object["filled_amount"]
        _commission = data_object["commission"]
        _average_price = data_object["average_price"]
        _price = data_object["price"]
        _direction = 1 if data_object["direction"] == "buy" else -1
        _amount = data_object["amount"]

        _full_ndarray = np.array(
            [
                _change_id,
                _creation_time,
                _last_update,
                _ins_idx,
                _instrument_strike,
                _instrument_maturity,
                _instrument_type,
                _order_type,
                _order_state,
                _order_id,
                _filled_amount,
                _commission,
                _average_price,
                _price,
                _direction,
                _amount,
            ]
        )
        return np.array(_full_ndarray)

    def _create_subscription_request(self):
        self.scrapper.send_new_request(
            MSG_LIST.auth_message(
                client_id=self.client_id, client_secret=self.client_secret
            )
        )

        self._user_orders_change_subscription_request()

    def _record_to_daemon_database_pipeline(
        self, record_dataframe: DataFrame, tag_of_data: str
    ) -> DataFrame:
        if "CHANGE_ID" in record_dataframe.columns:
            return record_dataframe.iloc[:, 1:]
        return record_dataframe

    def _user_orders_change_subscription_request(self):
        for _instrument_name in self.scrapper.instruments_list:
            subscription_message = (
                MSG_LIST.make_user_orders_subscription_request_by_instrument(
                    instrument_name=_instrument_name,
                )
            )
            self.scrapper.send_new_request(request=subscription_message)

        # Extra like BTC-PERPETUAL
        for _instrument_name in self.scrapper.configuration["orderBookScrapper"][
            "add_extra_instruments"
        ]:
            subscription_message = (
                MSG_LIST.make_user_orders_subscription_request_by_instrument(
                    instrument_name=_instrument_name,
                )
            )
            self.scrapper.send_new_request(request=subscription_message)
