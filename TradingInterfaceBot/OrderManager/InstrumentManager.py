# Class with instrument manager
import asyncio
from threading import Thread

from .AbstractInstrument import AbstractInstrument
from typing import TYPE_CHECKING, List, Dict, Final
import yaml
from pprint import pprint

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
    def __init__(self, interface: interface_typing, interface_cfg: dict, work_loop: asyncio.unix_events.SelectorEventLoop,
                 use_config: ConfigRoot = ConfigRoot.DIRECTORY,
                 strategy_configuration: dict = None):

        Thread.__init__(self)
        self.async_loop = work_loop

        self.interface = interface
        self.order_book_depth = interface_cfg["orderBookScrapper"]["depth"]
        self.managed_instruments = {}

        if use_config == ConfigRoot.DIRECTORY:
            cfg_path = "/".join(__file__.split('/')[:-1]) + "/" + "OrderManagerConfig.yaml"
            with open(cfg_path, "r") as ymlfile:
                self.configuration = yaml.load(ymlfile, Loader=yaml.FullLoader)

        elif use_config == ConfigRoot.STRATEGY:
            self.configuration = strategy_configuration
        else:
            raise ValueError('Wrong config source at InstrumentManager')

        asyncio.run_coroutine_threadsafe(self.validate_positions(), self.async_loop)
        self.initialize_instruments(self.interface.instruments_list)
        self.client_id = \
            self.interface.configuration["user_data"]["test_net"]["client_id"] \
                if self.interface.configuration["orderBookScrapper"]["test_net"] else \
                self.interface.configuration["user_data"]["production"]["client_id"]

        self.client_secret = \
            self.interface.configuration["user_data"]["test_net"]["client_secret"] \
                if self.interface.configuration["orderBookScrapper"]["test_net"] else \
                self.interface.configuration["user_data"]["production"]["client_secret"]

        self.interface.send_new_request(auth_message(client_id=self.client_id,
                                                    client_secret=self.client_secret))

    def initialize_instruments(self, instrument_names: List[str]):
        for instrument in instrument_names:
            self.managed_instruments[instrument] = \
                AbstractInstrument(
                    interface=self.interface,
                    instrument_name=instrument,
                    trades_buffer_size=self.configuration["OrderManager"]["BufferSizeForTrades"],
                    order_book_changes_buffer_size=self.configuration["OrderManager"]["BufferSizeForOrderBook"],
                    user_trades_buffer_size=self.configuration["OrderManager"]["BufferSizeForUserTrades"],
                )

    async def process_validation(self, callback: dict):
        # Process positions
        if all(key in callback for key in self._position_keys):
            instrument_name = callback["instrument_name"]
            print(self.managed_instruments[instrument_name])
        pass

    # TODO: implement
    async def validate_positions(self):
        """
        Валидирует записанные позиции по инструментам.
        Вызывается раз в какой-то промежуток времени для того чтобы быть уверенным в том
        что исполнение идет корректно
        :return:
        """
        while True:
            print("Call validation")
            await asyncio.sleep(self.configuration["OrderManager"]["validation_time"])
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

    def update_order_book(self, callback):
        pass

    def update_trade(self, callback):
        pass

    def update_user_trade(self, callback):
        pass

    def process_callback(self, callback):
        pass

    
if __name__ == '__main__':
    with open("/Users/molozey/Documents/DeribitDataScrapper/TradingInterfaceBot/configuration.yaml", "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    manager = InstrumentManager({}, cfg, ConfigRoot.DIRECTORY)
    pprint(manager.managed_instruments)
    manager.async_loop.run_forever()