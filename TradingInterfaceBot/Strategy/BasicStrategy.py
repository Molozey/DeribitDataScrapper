from TradingInterfaceBot.Strategy.AbstractStrategy import AbstractStrategy


class BaseStrategy(AbstractStrategy):
    async def on_order_book_update(self, callback: dict):
        pass

    async def on_trade_update(self, callback: dict):
        pass

    async def on_order_update(self, callback: dict):
        pass

    async def on_tick_update(self, callback: dict):
        print(callback)