from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
from TradingInterfaceBot.Utils import OrderStructure
from TradingInterfaceBot.InstrumentManager import AbstractInstrument

if TYPE_CHECKING:
    from TradingInterfaceBot.Scrapper.TradingInterface import DeribitClient
    scrapper_type = DeribitClient
else:
    scrapper_type = object


class AbstractStrategy(ABC):
    data_provider: scrapper_type
    open_orders: dict[int, OrderStructure] = dict()
    all_orders: dict[int, OrderStructure] = dict()

    def connect_client(self, data_provider: scrapper_type):
        self.data_provider = data_provider

    @abstractmethod
    async def on_order_book_update(self, abstractInstrument: AbstractInstrument):
        pass

    @abstractmethod
    async def on_trade_update(self, abstractInstrument: AbstractInstrument):
        pass

    @abstractmethod
    async def on_order_update(self, updatedOrder: OrderStructure):
        pass

    @abstractmethod
    async def on_tick_update(self, callback: dict):
        pass

    @abstractmethod
    async def on_position_miss_match(self):
        pass

    @abstractmethod
    async def on_not_enough_fund(self, callback: dict):
        pass

    @abstractmethod
    async def on_order_creation(self, createdOrder: OrderStructure):
        pass

    @abstractmethod
    async def price_too_high(self, callback: dict):
        pass

    @abstractmethod
    async def on_api_external_order(self, callback: dict):
        pass
