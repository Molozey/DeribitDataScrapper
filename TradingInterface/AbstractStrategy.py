from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypedDict
from dataclasses import dataclass
from enum import Enum

import AvailableInstruments
from MSG_LIST import order_request
if TYPE_CHECKING:
    from TradingInterface.TradingInterface import TradingDeribitInterface
    interface_type = TradingDeribitInterface
else:
    interface_type = object


class ActiveOrder(TypedDict):
    int: OrderStructure


class OrderDirection(Enum):
    LONG = 1
    SHORT = -1


class OrderState(Enum):
    deribit_name: str

    def __init__(self, deribit_naming: str):
        self.deribit_name = deribit_naming

    OPEN = "open"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    UNTRIGGERED = "untriggered"


class OrderType(Enum):
    deribit_name: str

    def __init__(self, deribit_naming: str):
        self.deribit_name = deribit_naming

    LIMIT = "limit"
    STOP_LIMIT = "stop_limit"
    TAKE_LIMIT = "take_limit"
    MARKET = "market"
    STOP_MARKET = "stop_market"
    TAKE_MARKET = "take_market"
    MARKET_LIMIT = "market_limit"
    TRAILING_STOP = "trailing_stop"


@dataclass()
class OrderUpdateCallback:
    price: float
    direction: OrderDirection
    order_type: OrderType
    order_state: OrderState
    order_id: int
    last_update_timestamp: int
    label: str
    price: float
    creation_timestamp: int
    instrument_name: str | AvailableInstruments.Instrument
    filled_amount: float
    amount: float


@dataclass()
class OrderStructure:
    control_interface: AbstractStrategy

    order_id: int | None
    open_time: int
    price: float | None
    executed_price: float
    total_commission: float
    direction: OrderDirection
    order_amount: float
    filled_amount: float
    last_update_time: int
    order_exist_time: int
    order_open_time: int
    order_border_time: int

    async def validate_order_exist_time(self):
        while True:
            if (self.order_exist_time - self.order_open_time) > self.order_border_time:
                self.control_interface.trading_interface.close_order(self)
                return 1

            await asyncio.sleep(0.5)


class AbstractStrategy(ABC):
    trading_interface: interface_type

    active_orders: dict[int, OrderStructure] = dict()
    ever_created_orders: dict[int, OrderStructure] = dict()

    def add_trading_interface(self, trading_interface: interface_type):
        self.trading_interface = trading_interface

    @abstractmethod
    def _process_order_update(self, order_update_callback: OrderUpdateCallback):
        pass

    @abstractmethod
    def _process_price_update(self, price_update_callback):
        pass

    def process_price_update(self, price_update_callback):
        self._process_price_update(price_update_callback)

    def process_order_update(self, order_update_callback: OrderUpdateCallback):
        self._process_order_update(order_update_callback)

    def cold_start_dev_func(self):
        self.trading_interface.send_new_request(request=order_request(
            order_side="buy",
            instrument_name="BTC-PERPETUAL",
            amount=10_00,
            order_type="market",
            order_price=21_134.5
        ))

class BaseStrategy(AbstractStrategy):
    def _process_order_update(self, order_update_callback: OrderUpdateCallback):
        print(order_update_callback)

    def _process_price_update(self, price_update_callback):
        pass
