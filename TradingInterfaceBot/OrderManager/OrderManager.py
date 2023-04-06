# Class with order manager
from AbstractInstrument import AbstractInstrument
from typing import TYPE_CHECKING, List
import yaml

from TradingInterfaceBot.Utils import ConfigRoot
if TYPE_CHECKING:
    from TradingInterfaceBot.Strategy import AbstractStrategy
    from TradingInterfaceBot.Scrapper.TradingInterface import DeribitClient
    strategy_typing = AbstractStrategy
    interface_typing = DeribitClient
else:
    strategy_typing = object
    interface_typing = object


class OrderManager:
    managed_instruments: List[AbstractInstrument]

    def __init__(self, interface_cfg: dict, use_config: ConfigRoot = ConfigRoot.DIRECTORY,
                 strategy_configuration: dict = None):
        self.order_book_depth = interface_cfg["orderBookScrapper"]["depth"]
        if use_config == ConfigRoot.DIRECTORY:
            cfg_path = "/".join(__file__.split('/')[:-1]) + "/" + "OrderManagerConfig.yaml"
            with open(cfg_path, "r") as ymlfile:
                self.configuration = yaml.load(ymlfile, Loader=yaml.FullLoader)
            print(self.configuration)

        elif use_config == ConfigRoot.STRATEGY:
            self.configuration = strategy_configuration
        else:
            raise ValueError('Wrong config source at OrderManager')


if __name__ == '__main__':
    manager = OrderManager({})
