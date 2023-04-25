import numpy as np

from typing import List, Tuple
from ..AbstractExternal import AbstractExternal
from ...InstrumentManager import AbstractInstrument


class SabrCalibration(AbstractExternal):
    def collect_data_from_instruments(self):
        recorded_instruments: Tuple[AbstractInstrument] = tuple(self.strategy.data_provider.instrument_manager.managed_instruments.values())
        unique_maturities = np.unique([instrument.get_raw_instrument_maturity() for instrument in recorded_instruments])
        print("Unique maturities", unique_maturities)

    async def on_order_book_update(self, abstractInstrument: AbstractInstrument):
        print("SABR ORDER BOOK UPDATE")
        self.collect_data_from_instruments()

    async def on_trade_update(self, abstractInstrument: AbstractInstrument):
        print("SABR TRADE UPDATE")
        self.collect_data_from_instruments()

    async def on_tick_update(self, callback: dict):
        print("SABR TICK UPDATE")

