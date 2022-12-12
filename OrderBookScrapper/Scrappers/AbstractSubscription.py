from abc import ABC, abstractmethod
from typing import List


def flatten(list_of_lists):
    if len(list_of_lists) == 0:
        return list_of_lists
    if isinstance(list_of_lists[0], list):
        return flatten(list_of_lists[0]) + flatten(list_of_lists[1:])
    return list_of_lists[:1] + flatten(list_of_lists[1:])


class AbstractSubscription(ABC):
    @abstractmethod
    def create_columns_list(self):
        pass

    @abstractmethod
    def create_subscription_request_message(self):
        pass

    @abstractmethod
    def _process_update_information_line(self, *args) -> list:
        pass

    @abstractmethod
    def _process_response(self):
        pass

    def process_response_from_server(self):
        return self._process_response()

    def process_update_information_line(self, *args) -> list:
        return self._process_update_information_line()


class OrderBookSubscription(AbstractSubscription):
    def __init__(self, order_book_depth: int):
        self.depth: int = order_book_depth

    def create_columns_list(self) -> List[str]:
        if self.depth == 0:
            raise NotImplementedError
        else:
            columns = ["CHANGE_ID", "NAME_INSTRUMENT", "TIMESTAMP_VALUE"]
            columns.extend(map(lambda x: [f"BID_{x}_PRICE", f"BID_{x}_AMOUNT"], range(self.depth)))
            columns.extend(map(lambda x: [f"ASK_{x}_PRICE", f"ASK_{x}_AMOUNT"], range(self.depth)))

            columns = flatten(columns)
            return columns


    def create_subscription_request_message(self):
        pass

    def _process_update_information_line(self, *args) -> list:
        pass