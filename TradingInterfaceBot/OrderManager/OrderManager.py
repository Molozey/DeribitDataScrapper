from abc import ABC
from typing import Dict, TYPE_CHECKING, List, Union
from TradingInterfaceBot.Utils import OrderStructure, OrderType, OrderState, CircularBuffer, \
    convert_deribit_order_type_to_structure, convert_deribit_order_status_to_structure, OrderSide, order_request

if TYPE_CHECKING:
    from TradingInterfaceBot.Scrapper.TradingInterface import DeribitClient

    deribitClientType = DeribitClient
else:
    deribitClientType = object


class OrderManager(ABC):
    open_orders: Dict[str, Union[OrderStructure, None]]  # Tag -> Structure
    filled_orders: Dict[str, OrderStructure]  # Tag -> Structure
    rejected_orders: Dict[str, OrderStructure]  # Tag -> Structure
    cancelled_orders: Dict[str, OrderStructure]  # Tag -> Structure
    untriggered_orders: Dict[str, OrderStructure]  # Tag -> Structure

    client: deribitClientType

    used_tags: CircularBuffer[str]

    instrument_to_tags_map: Dict[str, List[str]]

    def __init__(self):
        self.open_orders = dict()
        self.filled_orders = dict()
        self.rejected_orders = dict()
        self.cancelled_orders = dict()
        self.untriggered_orders = dict()
        self.instrument_to_tags_map = dict()

        self.used_tags = CircularBuffer(size=300)  # TODO: need to be tested. Probably may be to small
        self.used_tags.record(-1)

        self.client = None

        print("Order Manager has been initialized")

    def connect_client(self, client: deribitClientType):
        self.client = client

    def _create_order_tag(self, instrument_name: str) -> str:
        _prev_tag = int(self.used_tags[-1])
        _new_tag = _prev_tag + 1
        self.used_tags.record(_new_tag)
        if instrument_name not in self.instrument_to_tags_map:
            self.instrument_to_tags_map[instrument_name] = [f"{_new_tag}"]
        else:
            self.instrument_to_tags_map[instrument_name].append(f"{_new_tag}")
        return str(_new_tag)

    async def place_new_order(self, instrument_name: str, order_side: OrderSide, amount: float, order_type: OrderType,
                        order_price: float = None):
        _order_tag = self._create_order_tag(instrument_name=instrument_name)
        self.open_orders[f"{_order_tag}"] = None
        self.client.send_new_request(
            request=order_request(order_side=order_side, instrument_name=instrument_name,
                                  amount=amount, order_type=order_type, order_tag=_order_tag,
                                  order_price=order_price))

    async def process_order_callback(self, callback: dict):
        await self._extract_order_callback(callback=callback)

    async def _extract_order_callback(self, callback: dict):
        # Extract order tag for sub pipeline
        _tag = 'none'
        _callback_data = {}
        if 'params' in callback:
            _tag = callback["params"]["data"]["label"]
            _callback_data = callback["params"]["data"]
        elif 'result' in callback: # Extract order tag for initial pipeline
            if 'order' in callback['result']:
                _tag = callback['result']['order']['label']
                _callback_data = callback["result"]["order"]
        else:
            raise ValueError('no callback')
        _order_id, _open_time, _price, _executed_price, _total_commission, _direction, _order_amount, _filled_amount, _last_update_time, _order_exist_time, _instrument, _order_type, _order_state = await self._extract_values_from_callback(
            _callback_data)
        _order = await self.collect_order_object_by_tag(order_tag=_tag)
        if type(_order) == OrderStructure:
            # Order Exist in structures
            # TODO: implement
            _order.order_amount = _order_amount
            _order.last_update_time = _last_update_time
            _order.executed_price = _executed_price
            _order.filled_amount = _filled_amount
            _order.total_commission = _total_commission
            _order.order_exist_time = _order_exist_time
            _order.price = _price
            if _order.order_state != _order_state:
                await self.change_order_state(order_tag=_tag, newOrderState=_order_state)

            await self.client.connected_strategy.on_order_update(await self.collect_order_object_by_tag(order_tag=_tag))

        elif type(_order) == int:
            # Order Doesn't Exist in structures
            await self._if_order_dont_exist(
                order_tag=_tag, order_id=_order_id, open_time=_open_time, price=_price, executed_price=_executed_price,
                total_commission=_total_commission, direction=_direction, order_amount=_order_amount,
                filled_amount=_filled_amount, last_update_time=_last_update_time, instrument=_instrument,
                order_type=_order_type, order_state=_order_state, order_exist_time=_order_exist_time)
            pass
        else:
            raise ValueError("Unknown state of tag. Unable to define order status")

    async def _if_order_dont_exist(self, order_tag: str, order_id, open_time, price,
                             executed_price, total_commission, direction, order_amount,
                             filled_amount, last_update_time,
                             instrument, order_type, order_state, order_exist_time):
        _new_order = OrderStructure(order_tag=order_tag,
                                    order_id=order_id,
                                    open_time=open_time,
                                    price=price,
                                    executed_price=executed_price,
                                    total_commission=total_commission,
                                    direction=direction,
                                    order_amount=order_amount,
                                    filled_amount=filled_amount,
                                    last_update_time=last_update_time,
                                    order_exist_time=order_exist_time,
                                    instrument=instrument,
                                    order_type=order_type,
                                    order_state=order_state)
        # Place New order
        self.open_orders[f"{order_tag}"] = _new_order
        await self.client.connected_strategy.on_order_creation(_new_order)

    async def collect_order_object_by_tag(self, order_tag: str) -> Union[OrderStructure, int]:
        """
        Get order objects from structures. If no order in structures return -1
        :param order_tag:
        :return:
        """
        if order_tag in self.open_orders:
            _order = self.open_orders[f'{order_tag}']
        elif order_tag in self.filled_orders:
            _order = self.filled_orders[f'{order_tag}']
        elif order_tag in self.rejected_orders:
            _order = self.rejected_orders[f'{order_tag}']
        elif order_tag in self.cancelled_orders:
            _order = self.cancelled_orders[f'{order_tag}']
        elif order_tag in self.untriggered_orders:
            _order = self.untriggered_orders[f'{order_tag}']
        else:
            _order = -1
        if _order is None:
            _order = -1
        return _order

    async def change_order_state(self, order_tag: str, newOrderState: OrderState):
        # Collect order from structures
        if order_tag in self.open_orders:
            _order = self.open_orders[f'{order_tag}']
            if _order is not None:
                self.open_orders.pop(f'{order_tag}')
            else:
                raise ValueError('Cannot change state of only initialized order')
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

    async def _extract_values_from_callback(self, _callback_data):
        _order_id = _callback_data['order_id']
        _open_time = _callback_data['creation_timestamp']
        _price = _callback_data['price']
        _executed_price = _callback_data['average_price']
        _total_commission = _callback_data['commission']
        _direction = _callback_data['direction']
        _order_amount = _callback_data['amount']
        _filled_amount = _callback_data['filled_amount']
        _last_update_time = _callback_data['last_update_timestamp']
        _order_exist_time = _last_update_time - _open_time
        _instrument = _callback_data['instrument_name']
        _order_type = convert_deribit_order_type_to_structure(_callback_data["order_type"])
        _order_state = convert_deribit_order_status_to_structure(_callback_data["order_state"])
        return _order_id, _open_time, _price, _executed_price, _total_commission, _direction, _order_amount, \
               _filled_amount, _last_update_time, _order_exist_time, _instrument, _order_type, _order_state

    async def not_enough_funds(self, callback):
        # {'jsonrpc': '2.0', 'id': 5275, 'error': {'message': 'not_enough_funds', 'code': 10009}, 'usIn': 1682176942386379, 'usOut': 1682176942387152, 'usDiff': 773, 'testnet': True}
        await self.client.connected_strategy.on_not_enough_fund(callback=callback)

    async def price_too_high(self, callback):
        await self.client.connected_strategy.price_too_high(callback=callback)