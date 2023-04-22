# Class with instrument manager
import asyncio
import logging
from threading import Thread

from .AbstractInstrument import AbstractInstrument
from typing import TYPE_CHECKING, List, Dict, Final
import yaml

from TradingInterfaceBot.Utils import ConfigRoot, get_positions_request, Currency, auth_message

if TYPE_CHECKING:
    from TradingInterfaceBot.Strategy import AbstractStrategy
    from TradingInterfaceBot.Scrapper.TradingInterface import DeribitClient
    strategy_typing = AbstractStrategy
    interface_typing = DeribitClient
else:
    strategy_typing = object
    interface_typing = object


class InstrumentManager(Thread):
    managed_instruments: Dict[str, AbstractInstrument]
    interface: interface_typing

    _position_keys: Final = ('size_currency', 'size', 'realized_profit_loss', 'total_profit_loss')

    def __init__(self, interface: interface_typing, interface_cfg: dict,
                 work_loop: asyncio.unix_events.SelectorEventLoop,
                 use_config: ConfigRoot = ConfigRoot.DIRECTORY,
                 strategy_configuration: dict = None):

        Thread.__init__(self)
        self.async_loop = work_loop

        self.interface = interface
        self.order_book_depth = interface_cfg["orderBookScrapper"]["depth"]
        self.managed_instruments = {}
        if use_config == ConfigRoot.DIRECTORY:
            cfg_path = "/".join(__file__.split('/')[:-1]) + "/" + "InstrumentManagerConfig.yaml"
            with open(cfg_path, "r") as ymlfile:
                self.configuration = yaml.load(ymlfile, Loader=yaml.FullLoader)

        elif use_config == ConfigRoot.STRATEGY:
            self.configuration = strategy_configuration
        else:
            raise ValueError('Wrong config source at InstrumentManager')

        # Take auth data from configuration files
        self.client_id = \
            self.interface.configuration["user_data"]["test_net"]["client_id"] \
                if self.interface.configuration["orderBookScrapper"]["test_net"] else \
                self.interface.configuration["user_data"]["production"]["client_id"]

        self.client_secret = \
            self.interface.configuration["user_data"]["test_net"]["client_secret"] \
                if self.interface.configuration["orderBookScrapper"]["test_net"] else \
                self.interface.configuration["user_data"]["production"]["client_secret"]

        # Send auth request
        if not self.interface.auth_complete:
            self.interface.send_new_request(auth_message(client_id=self.client_id,
                                                         client_secret=self.client_secret))

        # Run coroutine with position infinite validation task.
        asyncio.run_coroutine_threadsafe(self.validate_positions(), self.async_loop)

        # Initialize all instruments
        self.initialize_instruments(self.interface.instruments_list)

    def initialize_instruments(self, instrument_names: List[str]):
        for instrument in instrument_names:
            params = {
                "instrument_name": f"{instrument}"
            }
            instrument_data = self.interface.send_block_sync_request(params,
                                                                     method='get_position',
                                                                     _private='private')
            # TODO: what we need to take Amount (in USD) or Value (in BTC)
            _cold_start_position = instrument_data["result"]["size"]
            self.managed_instruments[instrument] = \
                AbstractInstrument(
                    interface=self.interface,
                    instrument_name=instrument,
                    trades_buffer_size=self.configuration["InstrumentManager"]["BufferSizeForTrades"],
                    order_book_changes_buffer_size=self.configuration["InstrumentManager"]["BufferSizeForOrderBook"],
                    user_trades_buffer_size=self.configuration["InstrumentManager"]["BufferSizeForUserTrades"],
                    cold_start_user_position=_cold_start_position
                )

    async def process_validation(self, callback: dict):
        # Process positions
        if all(key in callback for key in self._position_keys):
            instrument_name = callback["instrument_name"]
            # Mismatch with sizes. TODO: what i should do if wrong?
            if self.managed_instruments[instrument_name] != callback["size"]:
                logging.error(f"Instrument {instrument_name} has mismatch in sizes | Recorded = {self.managed_instruments[instrument_name].user_position} | Real (Deribit Info) = {callback['size']}")
                # self.managed_instruments[instrument_name].user_position = callback['size']
                await self.interface.connected_strategy.on_position_miss_match()

    async def validate_positions(self):
        """
        Валидирует записанные позиции по инструментам.
        Вызывается раз в какой-то промежуток времени для того чтобы быть уверенным в том
        что исполнение идет корректно. (Позиция в абстрактном инструменте совпадает с тем, что выдает Deribit)
        :return:
        """
        while True:
            await asyncio.sleep(self.configuration["InstrumentManager"]["validation_time"])
            print("===" * 5 + "Call validation" + "===" * 5)
            self.interface.send_new_request(
                get_positions_request(Currency.BITCOIN, "future")
            )
            self.interface.send_new_request(
                get_positions_request(Currency.BITCOIN, "option")
            )
            self.interface.send_new_request(
                get_positions_request(Currency.ETHER, "future")
            )
            self.interface.send_new_request(
                get_positions_request(Currency.ETHER, "option")
            )

    async def update_order_book(self, callback):
        """
        В случае order book update
        :param callback:
        :return:
        """
        _order_book_change = callback['params']['data']

        _bid_prices = []
        _bid_amounts = []
        for _bid in _order_book_change["bids"]:
            _bid_prices.append(_bid[0])
            _bid_amounts.append(_bid[1])

        _ask_prices = []
        _ask_amounts = []
        for _ask in _order_book_change["asks"]:
            _ask_prices.append(_ask[0])
            _ask_amounts.append(_ask[1])

        self.managed_instruments[_order_book_change['instrument_name']].place_order_book_change(
            ask_prices=_ask_prices, ask_amounts=_ask_amounts,
            bid_prices=_bid_prices, bid_amounts=_bid_amounts,
            time=_order_book_change["timestamp"]
        )
        logging.info(f"Update orderBook at Instrument: {self.managed_instruments[_order_book_change['instrument_name']]}")
        await self.interface.connected_strategy.on_order_book_update(self.managed_instruments[_order_book_change['instrument_name']])

    async def update_trade(self, callback):
        """
        В случае нового trade (может быть user trade / может быть market trade)
        :param callback:
        :return:
        """
        for trade_object in callback['params']['data']:
            _amount = trade_object['amount'] if trade_object['direction'] == 'buy' else - trade_object['amount']
            self.managed_instruments[trade_object['instrument_name']].place_last_trade(
                trade_price=trade_object['price'], trade_amount=_amount, trade_time=trade_object['timestamp'])
            logging.info(f"Update trade at Instrument: {self.managed_instruments[trade_object['instrument_name']]}")
            await self.interface.connected_strategy.on_trade_update(
                self.managed_instruments[trade_object['instrument_name']]
            )

    async def update_user_trade(self, callback):
        """
        В случае user trade
        :param callback:
        :return:
        """
        pass

    def process_callback(self, callback):
        pass


if __name__ == '__main__':
    with open("/Users/molozey/Documents/DeribitDataScrapper/TradingInterfaceBot/configuration.yaml", "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    # loop = asyncio.new_event_loop()
    # manager = InstrumentManager({}, cfg, work_loop=asyncio.new_event_loop(), use_config=ConfigRoot.DIRECTORY)
    # pprint(manager.managed_instruments)
    # manager.async_loop.run_forever()