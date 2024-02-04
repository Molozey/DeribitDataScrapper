import logging
from abc import ABC
from datetime import datetime
from datetime import timedelta
from functools import cached_property
from time import time as sys_time
from typing import Final
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from deribit_data_scrapper.Utils import CircularBuffer
from deribit_data_scrapper.Utils import InstrumentType

if TYPE_CHECKING:
    from deribit_data_scrapper.Scrapper.TradingInterface import DeribitClient

    interface_type = DeribitClient
else:
    interface_type = object


class TradeInformation:
    __slots__ = {"trade_time", "trade_price", "trade_amount"}
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
        return str(
            {
                "trade_time": self.trade_time,
                "trade_price": self.trade_price,
                "trade_amount": self.trade_amount,
            }
        )


class OrderBookChange:
    """Class for storing order book changes"""

    __slots__ = {
        "order_book_change_time",
        "ask_prices",
        "ask_amounts",
        "bid_prices",
        "bid_amounts",
    }
    order_book_change_time: Final[float]
    ask_prices: Final[List[float]]
    ask_amounts: Final[List[float]]
    bid_prices: Final[List[float]]
    bid_amounts: Final[List[float]]

    def __init__(
        self,
        ask_prices: List[float],
        ask_amounts: List[float],
        bid_prices: List[float],
        bid_amounts: List[float],
        time: float = None,
    ):
        self.ask_prices = ask_prices
        self.ask_amounts = ask_amounts
        self.bid_prices = bid_prices
        self.bid_amounts = bid_amounts
        if time:
            self.order_book_change_time = time
        else:
            self.order_book_change_time = sys_time() * 1000


class AbstractInstrumentInfo(ABC):
    """
    Abstract class for information about instrument. Contains all information about instrument strike, maturity, type, name
    """

    instrument_name: str
    instrument_type: Optional[InstrumentType]

    _instrument_strike: Optional[float] = None  # Only for options | futures
    _instrument_maturity: Optional[datetime] = None  # Only for options | futures

    def __init__(self, instrument_name: str):
        self._instrument_strike = None
        self._instrument_maturity = None
        self.instrument_type = None

        self.instrument_name = instrument_name

        self.parse_instrument_name()  # Parse instrument name and extract all need info

    @cached_property
    def get_int_instrument_index(self):
        if "BTC" in self.instrument_name:
            return 0
        elif "ETH" in self.instrument_name:
            return 1

    def get_fields(self) -> [int, float, int, int]:
        """
        Return fields of instrument. Used for saving to database. Order of fields is important.
        :return: ins_index, strike, maturity, type
        """
        if self._instrument_strike is not None:
            _strike = self._instrument_strike
        else:
            _strike = -1.0

        if self._instrument_maturity is not None:
            _maturity = self._instrument_maturity.timestamp()
        else:
            _maturity = -1

        if self.instrument_type is not None:
            _type = self.instrument_type.number
        else:
            _type = -1
        return [self.get_int_instrument_index, _strike, _maturity, _type]

    @property
    def instrument_strike(self):
        """Instrument strike. Only for options | futures. For other instruments return -1"""
        if (
            (self.instrument_type == InstrumentType.CALL_OPTION)
            or (self.instrument_type == InstrumentType.PUT_OPTION)
            or (self.instrument_type == InstrumentType.FUTURE)
        ):
            return float(self._instrument_strike)
        else:
            logging.error(
                "Called strike attribute for instrument without strike. Check your logic!"
            )
            raise ValueError(f"No strike for instrument {self.instrument_name}")

    @property
    def instrument_maturity(self) -> float:
        if (
            (self.instrument_type == InstrumentType.CALL_OPTION)
            or (self.instrument_type == InstrumentType.PUT_OPTION)
            or (self.instrument_type == InstrumentType.FUTURE)
        ):
            return (self._instrument_maturity - datetime.now()) / timedelta(days=365)
        else:
            logging.error(
                "Called maturity attribute for instrument without maturity. Check your logic!"
            )
            raise ValueError(f"No maturity for instrument {self.instrument_name}")

    def get_raw_instrument_maturity(self):
        """
        return raw datetime of instrument maturity
        :return:
        """
        return self._instrument_maturity

    def __repr__(self):
        if (
            (self.instrument_type == InstrumentType.CALL_OPTION)
            or (self.instrument_type == InstrumentType.PUT_OPTION)
            or (self.instrument_type == InstrumentType.OPTION)
        ):
            return str(
                {
                    "instrument_name": self.instrument_name,
                    "instrument_type": self.instrument_type.instrument_type,
                    "instrument_strike": self.instrument_strike,
                    "instrument_maturity": self.instrument_maturity,
                }
            )
        elif self.instrument_type == InstrumentType.FUTURE:
            return str(
                {
                    "instrument_name": self.instrument_name,
                    "instrument_type": self.instrument_type.instrument_type,
                    "instrument_maturity": self.instrument_maturity,
                }
            )
        else:
            return str(
                {
                    "instrument_name": self.instrument_name,
                    "instrument_type": self.instrument_type.instrument_type,
                }
            )

    def parse_instrument_name(self):
        """
        Parse instrument name and extract all need info. For options and futures extract strike and maturity
        # TODO: no combo futures implemented.
        :return:
        """
        try:
            _maturity = None
            _strike = None
            _kind = InstrumentType.ASSET

            split_instrument_name = self.instrument_name.split("-")
            if split_instrument_name[-1] == "C":
                _kind = InstrumentType.CALL_OPTION
                _maturity = datetime.strptime(split_instrument_name[1], "%d%b%y")
                _strike = split_instrument_name[2]

            elif split_instrument_name[-1] == "P":
                _kind = InstrumentType.PUT_OPTION
                _maturity = datetime.strptime(split_instrument_name[1], "%d%b%y")
                _strike = split_instrument_name[2]
            else:
                if split_instrument_name[-1] == "PERPETUAL":
                    pass
                else:
                    _maturity = datetime.strptime(split_instrument_name[1], "%d%b%y")
                    _kind = InstrumentType.FUTURE

            self._instrument_maturity = _maturity
            self._instrument_strike = _strike
            self.instrument_type = _kind
        except:
            logging.error("Error while parsing instrument name")
            raise ValueError("Error while parsing instrument name")


class AbstractInstrument(ABC):
    """Abstract class for instrument. Contains all information about instrument and interface for trading.
    Also contains buffers for last trades and order book changes and user position
    """

    interface: Union[interface_type]

    last_trades: CircularBuffer[TradeInformation]
    last_order_book_changes: CircularBuffer[OrderBookChange]

    instrument_name: str
    instrument_type: InstrumentType
    user_position: float
    user_last_trades: CircularBuffer[TradeInformation]

    _instrument_strike: Optional[float] = None  # Only for options | futures
    _instrument_maturity: Optional[datetime] = None  # Only for options | futures

    def __init__(
        self,
        interface: interface_type,
        instrument_name: str,
        trades_buffer_size: int,
        order_book_changes_buffer_size: int,
        user_trades_buffer_size: int,
        cold_start_user_position: float = 0.0,
    ):
        self._instrument_strike = None
        self._instrument_maturity = None

        self.interface = interface
        self.instrument_name = instrument_name
        self.last_trades = CircularBuffer(size=trades_buffer_size)
        self.last_order_book_changes = CircularBuffer(
            size=order_book_changes_buffer_size
        )

        # User info
        self.user_position = cold_start_user_position
        self.user_last_trades = CircularBuffer(size=user_trades_buffer_size)

        self.parse_instrument_name()  # Parse instrument name and extract all need info

    def fill_trades_by_cold_start(self, trades_start_data: list[list]):
        """
        Fill trades buffer by cold start data
        :param trades_start_data: Array of [Trade Price, Trade Amount, Trade Time]
        :return:
        """
        for trade in trades_start_data:
            self.last_trades.record(
                TradeInformation(price=trade[0], amount=trade[1], time=trade[2])
            )

    def fill_order_book_by_cold_start(self):
        """
        Fill order book buffer by cold start data
        :return:
        """
        logging.error("NotImplementedError")
        raise NotImplementedError

    @property
    def instrument_strike(self):
        """
        Return strike of instrument. Only for options and futures
        :return:
        """
        if (
            (self.instrument_type == InstrumentType.CALL_OPTION)
            or (self.instrument_type == InstrumentType.PUT_OPTION)
            or (self.instrument_type == InstrumentType.FUTURE)
        ):
            return float(self._instrument_strike)
        else:
            logging.error(
                "Called strike attribute for instrument without strike. Check your logic!"
            )
            raise ValueError(f"No strike for instrument {self.instrument_name}")

    @property
    def instrument_maturity(self) -> float:
        """
        Return maturity of instrument. Only for options and futures
        :return:
        """
        if (
            (self.instrument_type == InstrumentType.CALL_OPTION)
            or (self.instrument_type == InstrumentType.PUT_OPTION)
            or (self.instrument_type == InstrumentType.FUTURE)
        ):
            return (self._instrument_maturity - datetime.now()) / timedelta(days=365)
        else:
            logging.error(
                "Called maturity attribute for instrument without maturity. Check your logic!"
            )
            raise ValueError(f"No maturity for instrument {self.instrument_name}")

    def get_raw_instrument_maturity(self):
        """
        return raw datetime of instrument maturity
        :return:
        """
        return self._instrument_maturity

    def place_last_trade(
        self, trade_price: float, trade_amount: float, trade_time: float = None
    ):
        """
        Place last trade to trades buffer
        :param trade_price: type float
        :param trade_amount: type float
        :param trade_time: type float
        :return:
        """
        self.last_trades.record(
            TradeInformation(price=trade_price, amount=trade_amount, time=trade_time)
        )

    def place_order_book_change(
        self,
        ask_prices: List[float],
        ask_amounts: List[float],
        bid_prices: List[float],
        bid_amounts: List[float],
        time: float = None,
    ):
        """
        Place order book change to order book buffer
        :param ask_prices: list of ask prices. type float
        :param ask_amounts: list of ask amounts. type float
        :param bid_prices: list of bid prices. type float
        :param bid_amounts: list of bid amounts. type float
        :param time: type float
        :return:
        """
        self.last_order_book_changes.record(
            OrderBookChange(
                ask_prices=ask_prices,
                ask_amounts=ask_amounts,
                bid_prices=bid_prices,
                bid_amounts=bid_amounts,
                time=time,
            )
        )

    # TODO: delete
    def connect_interface(self, interface: interface_type):
        self.interface = interface

    def __repr__(self):
        if (
            (self.instrument_type == InstrumentType.CALL_OPTION)
            or (self.instrument_type == InstrumentType.PUT_OPTION)
            or (self.instrument_type == InstrumentType.OPTION)
        ):
            return str(
                {
                    "instrument_name": self.instrument_name,
                    "instrument_type": self.instrument_type.instrument_type,
                    "instrument_strike": self.instrument_strike,
                    "instrument_maturity": self.instrument_maturity,
                    "last_orderBook_changes": self.last_order_book_changes,
                    "last_trades": self.last_trades,
                    "last_user_trades": self.user_last_trades,
                    "user_position": self.user_position,
                }
            )
        elif self.instrument_type == InstrumentType.FUTURE:
            return str(
                {
                    "instrument_name": self.instrument_name,
                    "instrument_type": self.instrument_type.instrument_type,
                    "instrument_maturity": self.instrument_maturity,
                    "last_orderBook_changes": self.last_order_book_changes,
                    "last_trades": self.last_trades,
                    "last_user_trades": self.user_last_trades,
                    "user_position": self.user_position,
                }
            )
        else:
            return str(
                {
                    "instrument_name": self.instrument_name,
                    "instrument_type": self.instrument_type.instrument_type,
                    "last_orderBook_changes": self.last_order_book_changes,
                    "last_trades": self.last_trades,
                    "last_user_trades": self.user_last_trades,
                    "user_position": self.user_position,
                }
            )

    def parse_instrument_name(self):
        """
        Parse instrument name and extract all need info.
        :return:
        """
        try:
            _maturity = None
            _strike = None
            _kind = InstrumentType.ASSET

            split_instrument_name = self.instrument_name.split("-")
            if split_instrument_name[-1] == "C":
                _kind = InstrumentType.CALL_OPTION
                _maturity = datetime.strptime(split_instrument_name[1], "%d%b%y")
                _strike = split_instrument_name[2]

            elif split_instrument_name[-1] == "P":
                _kind = InstrumentType.PUT_OPTION
                _maturity = datetime.strptime(split_instrument_name[1], "%d%b%y")
                _strike = split_instrument_name[2]
            else:
                if split_instrument_name[-1] == "PERPETUAL":
                    pass
                else:
                    _maturity = datetime.strptime(split_instrument_name[1], "%d%b%y")
                    _kind = InstrumentType.FUTURE

            self._instrument_maturity = _maturity
            self._instrument_strike = _strike
            self.instrument_type = _kind
        except:
            logging.error("Error while parsing instrument name")
            raise ValueError("Error while parsing instrument name")


if __name__ == "__main__":
    names = [
        "BTC-30JUN23-40000-P",
        "BTC-30JUN23-100000-P",
        "BTC-30JUN23-20000-C",
        "BTC-PERPETUAL",
    ]
    for name in names:
        ab = AbstractInstrument(None, name, 1, 1, 1, 0)
        print(ab)
