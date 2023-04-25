from abc import ABC, abstractmethod
from TradingInterfaceBot.InstrumentManager import AbstractInstrument


class AbstractExternal(ABC):
    @abstractmethod
    async def on_order_book_update(self, abstractInstrument: AbstractInstrument):
        pass

    @abstractmethod
    async def on_trade_update(self, abstractInstrument: AbstractInstrument):
        pass
