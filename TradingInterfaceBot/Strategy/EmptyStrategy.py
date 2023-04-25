import logging
from typing import Optional

from TradingInterfaceBot.InstrumentManager import AbstractInstrument
from TradingInterfaceBot.Strategy.AbstractStrategy import AbstractStrategy
from TradingInterfaceBot.Utils import OrderStructure, OrderType, TickerNode
import yaml


class EmptyStrategy(AbstractStrategy):
    def __init__(self, configuration_file: str = 'StrategyConfig'):
        if configuration_file == 'StrategyConfig':
            logging.info('Use default strategy configuration')

        cfg_path = "/".join(__file__.split('/')[:-1]) + "/" + f"{configuration_file}.yaml"
        with open(cfg_path, "r") as ymlfile:
            self.configuration = yaml.load(ymlfile, Loader=yaml.FullLoader)

        ticker_node = TickerNode(ping_time=self.configuration["StrategyConfiguration"]["TickerNodeFrequency"],
                                 wait_parameter=self.configuration["StrategyConfiguration"]["TickerNodeWaitingSize"])
        ticker_node.connect_strategy(plug_strategy=self)
        ticker_node.run_ticker_node()

    async def on_order_book_update(self, abstractInstrument: AbstractInstrument):
        # print("==== ON ORDER BOOK UPDATE ====")
        # print("Instrument: ", abstractInstrument.instrument_name, " New BID:",
        #       abstractInstrument.last_order_book_changes[-1].bid_prices, " NEW BID AMOUNT:",
        #       abstractInstrument.last_order_book_changes[-1].bid_amounts, " NEW ASK:",
        #       abstractInstrument.last_order_book_changes[-1].ask_prices, "NEW ASK AMOUNT: ",
        #       abstractInstrument.last_order_book_changes[-1].ask_amounts)
        #
        # print("******************************")
        pass

    async def on_trade_update(self, abstractInstrument: AbstractInstrument):
        # print("==== ON GENERAL TRADE UPDATE ====")
        # print("Instrument: ", abstractInstrument.instrument_name, " TRADE PRICE:",
        #       abstractInstrument.last_trades[-1].trade_price, " TRADE AMOUNT:",
        #       abstractInstrument.last_trades[-1].trade_amount)
        # print("******************************")
        pass

    async def on_order_update(self, updatedOrder: OrderStructure):
        print("==== ON ORDER UPDATE ====")

    async def on_order_creation(self, createdOrder: OrderStructure):
        print("==== ON ORDER CREATION ====")
        print("Created order", createdOrder)

    async def on_tick_update(self, callback: dict):
        print("==== ON TICK UPDATE ====")

    async def on_position_miss_match(self):
        print("==== POSITION MISS MATCH ====")

    async def on_not_enough_fund(self, callback: dict):
        print("==== NOT ENOUGH FUNDS ====")

    async def price_too_high(self, callback: dict):
        print("==== PRICE TOO HIGH ====")

    async def on_api_external_order(self, callback: dict):
        print("==== ORDER PLACED WITHOUT API ==== ")

if __name__ == '__main__':
    empty = EmptyStrategy()
