import logging
from abc import ABC, abstractmethod
from functools import partial
from typing import List, TYPE_CHECKING

from OrderBookScrapper.DataBase.mysqlRecording.cleanUpRequestsLimited import \
    REQUEST_TO_CREATE_LIMITED_ORDER_BOOK_CONTENT
from OrderBookScrapper.Utils import MSG_LIST

if TYPE_CHECKING:
    from OrderBookScrapper.Scrappers.DeribitClient import DeribitClient
    scrapper_typing = DeribitClient
else:
    scrapper_typing = object


def flatten(list_of_lists):
    if len(list_of_lists) == 0:
        return list_of_lists
    if isinstance(list_of_lists[0], list):
        return flatten(list_of_lists[0]) + flatten(list_of_lists[1:])
    return list_of_lists[:1] + flatten(list_of_lists[1:])


class AbstractSubscription(ABC):
    tables_names: List[str]
    tables_names_creation: List[str]

    def __init__(self, scrapper: scrapper_typing):
        self.scrapper = scrapper

    @abstractmethod
    def create_columns_list(self) -> list[str]:
        pass

    @abstractmethod
    def create_subscription_request(self) -> str:
        pass

    @abstractmethod
    def _process_update_information_line(self, *args) -> list:
        pass

    @abstractmethod
    def _process_response(self, response: dict):
        pass

    def process_response_from_server(self, response: dict):
        return self._process_response(response=response)

    def process_update_information_line(self, *args) -> list:
        return self._process_update_information_line()


class OrderBookSubscriptionCONSTANT(AbstractSubscription):
    tables_names = ["TABLE_DEPTH_{}"]

    # TODO:
    # tables_names = ["order_book_content", "script_snapshot_id", "script_snapshot_id"]

    def __init__(self, scrapper: scrapper_typing, order_book_depth: int):
        super(OrderBookSubscriptionCONSTANT, self).__init__(scrapper=scrapper)
        self.depth: int = order_book_depth
        self.tables_names = [f"TABLE_DEPTH_{self.depth}"]
        self.tables_names_creation = list(map(partial(REQUEST_TO_CREATE_LIMITED_ORDER_BOOK_CONTENT,
                                                      depth_size=self.depth), self.tables_names))

    def create_columns_list(self) -> List[str]:
        if self.depth == 0:
            raise NotImplementedError
        else:
            columns = ["CHANGE_ID", "NAME_INSTRUMENT", "TIMESTAMP_VALUE"]
            columns.extend(map(lambda x: [f"BID_{x}_PRICE", f"BID_{x}_AMOUNT"], range(self.depth)))
            columns.extend(map(lambda x: [f"ASK_{x}_PRICE", f"ASK_{x}_AMOUNT"], range(self.depth)))

            columns = flatten(columns)
            return columns

    def _process_update_information_line(self, *args) -> list:
        pass

    def _process_response(self, response: dict):
        # SUBSCRIPTION processing
        if response['method'] == "subscription":

            # ORDER BOOK processing. For constant book depth
            if 'change' and 'type' not in response['params']['data']:
                if self.scrapper.database:
                    self.scrapper.database.add_order_book_content_limited_depth(
                        change_id=response['params']['data']['change_id'],
                        timestamp=response['params']['data']['timestamp'],
                        bids=response['params']['data']['bids'],
                        asks=response['params']['data']['asks'],
                        instrument_name=response['params']['data']['instrument_name']
                    )
                return
            #
            # # INITIAL SNAPSHOT processing. For unlimited book depth
            # if response['params']['data']['type'] == 'snapshot':
            #     if self.scrapper.database:
            #         self.scrapper.database.add_instrument_init_snapshot(
            #             instrument_name=response['params']['data']['instrument_name'],
            #             start_instrument_scrap_time=response['params']['data']['timestamp'],
            #             request_change_id=response['params']['data']['change_id'],
            #             bids_list=response['params']['data']['bids'],
            #             asks_list=response['params']['data']['asks'],
            #         )
            #         return
            # # CHANGE ORDER BOOK processing. For unlimited book depth
            # if response['params']['data']['type'] == 'change':
            #     if self.scrapper.database:
            #         self.scrapper.database.add_instrument_change_order_book_unlimited_depth(
            #             request_change_id=response['params']['data']['change_id'],
            #             request_previous_change_id=response['params']['data']['prev_change_id'],
            #             change_timestamp=response['params']['data']['timestamp'],
            #             bids_list=response['params']['data']['bids'],
            #             asks_list=response['params']['data']['asks'],
            #         )
            #         return

    def make_new_subscribe_constant_depth_book(self, instrument_name: str,
                                               type_of_data="book",
                                               interval="100ms",
                                               depth=None,
                                               group=None):
        if instrument_name not in self.scrapper.instrument_requested:
            subscription_message = MSG_LIST.make_subscription_constant_book_depth(instrument_name,
                                                                                  type_of_data=type_of_data,
                                                                                  interval=interval,
                                                                                  depth=depth,
                                                                                  group=group)

            self.scrapper.send_new_request(request=subscription_message)
            self.scrapper.instrument_requested.add(instrument_name)
        else:
            logging.warning(f"Instrument {instrument_name} already subscribed")

    def create_subscription_request(self):
        print("Start")
        # Set heartbeat
        self.scrapper.send_new_request(MSG_LIST.set_heartbeat(
            self.scrapper.configuration["orderBookScrapper"]["hearth_beat_time"]))
        # Send all subscriptions
        for _instrument_name in self.scrapper.instruments_list:
            self.make_new_subscribe_constant_depth_book(instrument_name=_instrument_name,
                                                             depth=self.scrapper.configuration["orderBookScrapper"]["depth"],
                                                             group=self.scrapper.configuration["orderBookScrapper"]["group_in_limited_order_book"])

        # Extra like BTC-PERPETUAL
        for _instrument_name in self.scrapper.configuration["orderBookScrapper"]["add_extra_instruments"]:
            print("Extra:", _instrument_name)
            self.make_new_subscribe_constant_depth_book(instrument_name=_instrument_name,
                                                                 depth=self.scrapper.configuration["orderBookScrapper"]["depth"],
                                                                 group=self.scrapper.configuration["orderBookScrapper"]["group_in_limited_order_book"])