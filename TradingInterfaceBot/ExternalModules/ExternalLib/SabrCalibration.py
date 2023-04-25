from ..AbstractExternal import AbstractExternal
from ...InstrumentManager import AbstractInstrument


class SabrCalibration(AbstractExternal):
    async def on_order_book_update(self, abstractInstrument: AbstractInstrument):
        pass

    async def on_trade_update(self, abstractInstrument: AbstractInstrument):
        pass