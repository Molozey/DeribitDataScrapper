from typing import Optional

from TradingInterfaceBot.Strategy.AbstractStrategy import AbstractStrategy
from TradingInterfaceBot.Utils import OrderStructure, OrderType


class EmptyStrategy(AbstractStrategy):
    async def on_order_book_update(self, callback: dict):
        pass

    async def on_trade_update(self, callback: dict):
        pass

    async def on_order_update(self, callback: dict):
        pass

    async def on_tick_update(self, callback: dict):
        pass

    async def on_position_miss_match(self):
        pass

    async def on_not_enough_fund(self, callback: dict):
        print("==== NOT ENOUGH FUNDS ====")
