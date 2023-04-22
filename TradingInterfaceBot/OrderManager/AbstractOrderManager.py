from abc import ABC, abstractmethod
from typing import Dict, TYPE_CHECKING
from TradingInterfaceBot.Utils import OrderStructure, OrderType, OrderState, CircularBuffer

if TYPE_CHECKING:
    from TradingInterfaceBot.Scrapper.TradingInterface import DeribitClient
    deribitClientType = DeribitClient
else:
    deribitClientType = object


class AbstractOrderManager(ABC):
    open_orders: Dict[str, OrderStructure] # Tag -> Structure
    filled_orders: Dict[str, OrderStructure] # Tag -> Structure
    rejected_orders: Dict[str, OrderStructure] # Tag -> Structure
    cancelled_orders: Dict[str, OrderStructure] # Tag -> Structure
    untriggered_orders: Dict[str, OrderStructure] # Tag -> Structure

    client: deribitClientType

    used_tags: CircularBuffer[str]

    def __init__(self):
        self.open_orders = dict()
        self.filled_orders = dict()
        self.rejected_orders = dict()
        self.cancelled_orders = dict()
        self.untriggered_orders = dict()

        self.used_tags = CircularBuffer(size=300)   # TODO: need to be tested. Probably may be to small

    def connect_client(self, client: deribitClientType):
        self.client = client

    def _create_order_tag(self) -> int:
        _prev_tag = int(self.used_tags[-1])
        self.used_tags.record(_prev_tag + 1)
        return _prev_tag + 1

    def place_order(self):
        pass

    def process_order_callback(self, callback: dict):
        pass

    def _extract_order_callback(self, callback: dict):
        # Extract order tag
        _tag = callback["params"]["data"]["label"]

    def _if_order_dont_exist(self, order_tag: str):
        pass

    def change_order_state(self, order_tag: str, newOrderState: OrderState):
        # Collect order from structures
        if order_tag in self.open_orders:
            _order = self.open_orders[f'{order_tag}']
            self.open_orders.pop(f'{order_tag}')
        elif order_tag in self.filled_orders:
            _order = self.filled_orders[f'{order_tag}']
            self.filled_orders.pop(f'{order_tag}')
        elif order_tag in self.rejected_orders:
            _order = self.rejected_orders[f'{order_tag}']
            self.rejected_orders.pop(f'{order_tag}')
        elif order_tag in self.cancelled_orders:
            _order = self.cancelled_orders[f'{order_tag}']
            self.cancelled_orders.pop(f'{order_tag}')
        elif order_tag in self.untriggered_orders:
            _order = self.untriggered_orders[f'{order_tag}']
            self.untriggered_orders.pop(f'{order_tag}')
        else:
            raise MemoryError("Existed Order Tag doesn't have match in Order storage structures")

        # Change order state
        _order.order_state = newOrderState
        match newOrderState:
            case OrderState.OPEN:
                self.open_orders[f"{order_tag}"] = _order
            case OrderState.CANCELLED:
                self.cancelled_orders[f"{order_tag}"] = _order
            case OrderState.FILLED:
                self.filled_orders[f"{order_tag}"] = _order
            case OrderState.REJECTED:
                self.rejected_orders[f"{order_tag}"] = _order
            case _:
                raise ValueError('Unknown Order State')
