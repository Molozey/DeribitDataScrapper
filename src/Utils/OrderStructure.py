from dataclasses import dataclass
from enum import Enum


class OrderSide(Enum):
    BUY = 1,
    SELL = -1


class OrderType(Enum):
    """
    Order type enum class to convert deribit naming to structure naming.
    """
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


class OrderState(Enum):
    """
    Order state enum class to convert deribit naming to structure naming.
    """
    deribit_name: str

    def __init__(self, deribit_naming: str):
        self.deribit_name = deribit_naming

    OPEN = "open"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    UNTRIGGERED = "untriggered"


def convert_deribit_order_type_to_structure(der_ans: str) -> OrderType:
    """
    Convert deribit order type to structure order type.
    :param der_ans:
    :return:
    """
    match der_ans:
        case "limit":
            return OrderType.LIMIT
        case "market":
            return OrderType.MARKET
        case _:
            raise NotImplementedError


def convert_deribit_order_status_to_structure(der_ans: str) -> OrderState:
    """
    Convert deribit order status to structure order status.
    :param der_ans:
    :return:
    """
    match der_ans:
        case "open":
            return OrderState.OPEN
        case "filled":
            return OrderState.FILLED
        case "rejected":
            return OrderState.REJECTED
        case "cancelled":
            return OrderState.CANCELLED
        case _:
            raise NotImplementedError


@dataclass()
class OrderStructure:
    """
    Order structure to store order data
    """
    order_tag: str  # Order tag to identify order. Specified by user.
    order_id: int | None    # Order id
    open_time: int  # Order open time
    price: float | None     # Order price
    executed_price: float   # Order executed price
    total_commission: float     # Total commission
    direction: str  # Order direction
    order_amount: float    # Order amount
    filled_amount: float    # Filled amount
    last_update_time: int   # Last update time
    order_exist_time: int   # Order exist time
    instrument: str    # Instrument name
    order_type: OrderType   # Order type. See OrderType class
    order_state: OrderState    # Order state. See OrderState class
