from time import time as sys_time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Union, List, Final
from TradingInterfaceBot.Utils import CircularBuffer


if TYPE_CHECKING:
    from TradingInterfaceBot.Scrapper.TradingInterface import DeribitClient
    interface_type = DeribitClient
else:
    interface_type = object


class TradeInformation:
    __slots__ = {'trade_time', 'trade_price', 'trade_amount'}
    trade_time: Final[float]
    trade_price: Final[float]
    trade_amount: Final[float]

    def __init__(self, price: float, amount: float, time: float = None):
        self.trade_price = price
        self.trade_amount = amount
        if time:
            self.trade_time = time
        else:
            self.trade_time = sys_time() * 1000

    def __repr__(self):
        return str({"trade_time": self.trade_time, "trade_price": self.trade_price, "trade_amount": self.trade_amount})


class OrderBookChange:
    __slots__ = {'order_book_change_time', 'ask_prices', 'ask_amounts', 'bid_prices', 'bid_amounts'}
    order_book_change_time: Final[float]
    ask_prices: Final[List[float]]
    ask_amounts: Final[List[float]]
    bid_prices: Final[List[float]]
    bid_amounts: Final[List[float]]

    def __init__(self, ask_prices: List[float], ask_amounts: List[float],
                 bid_prices: List[float], bid_amounts: List[float], time: float = None):
        self.ask_prices = ask_prices
        self.ask_amounts = ask_amounts
        self.bid_prices = bid_prices
        self.bid_amounts = bid_amounts
        if time:
            self.order_book_change_time = time
        else:
            self.order_book_change_time = sys_time() * 1000


class AbstractInstrument(ABC):
    interface: Union[interface_type]

    last_trades: CircularBuffer[TradeInformation]
    last_order_book_changes: CircularBuffer[OrderBookChange]

    instrument_name: str
    user_position: float
    user_last_trades: CircularBuffer[TradeInformation]

    def __init__(self, interface: interface_type,
                 instrument_name: str, trades_buffer_size: int, order_book_changes_buffer_size: int,
                 user_trades_buffer_size: int):
        self.interface = interface
        self.instrument_name = instrument_name
        self.last_trades = CircularBuffer(size=trades_buffer_size)
        self.last_order_book_changes = CircularBuffer(size=order_book_changes_buffer_size)

        # User info
        self.user_position = 0
        self.user_last_trades = CircularBuffer(size=user_trades_buffer_size)

    def place_last_trade(self, trade_price: float, trade_amount: float, trade_time: float = None):
        self.last_trades.record(TradeInformation(price=trade_price, amount=trade_amount, time=trade_time))

    def place_order_book_change(self, ask_prices: List[float], ask_amounts: List[float],
                                bid_prices: List[float], bid_amounts: List[float], time: float = None):
        self.last_order_book_changes.record(
            OrderBookChange(ask_prices=ask_prices, ask_amounts=ask_amounts, bid_prices=bid_prices,
                            bid_amounts=bid_amounts, time=time)
        )

    # TODO: delete
    def connect_interface(self, interface: interface_type):
        self.interface = interface

    def __repr__(self):
        return str({
            'instrument_name': self.instrument_name,

            'last_orderBook_changes': self.last_order_book_changes,
            'last_trades': self.last_trades,

            'last_user_trades': self.user_last_trades,
            'user_position': self.user_position,
        })

if __name__ == '__main__':
    pass