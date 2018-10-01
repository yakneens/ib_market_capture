import asyncio
from ib_insync import *
import util.connection as db
from collections import namedtuple
import logging
import util.logging as my_log
import datetime

LiveFuturesSettings = namedtuple('LiveFuturesSettings',
                             'tickers, exchange, bar_size, whatToShow, influx_measurement, log_filename,')

class LiveFuturesGetter:
    def __init__(self, ib: IB, futures_settings: LiveFuturesSettings):
        self.settings = futures_settings

        self.logger = my_log.SetupLogger(self.settings.log_filename)
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib

        #These help keep track of which event handler applies
        self.request_ids = []

        if not self.onBarUpdate in self.ib.barUpdateEvent:
            self.ib.barUpdateEvent.connect(self.onBarUpdate, weakRef=False)



    async def save_to_influx(self, bar, contract):
        new_bar = util.df([bar])
        new_bar = new_bar.set_index('time')
        await db.influx_client.write(new_bar,
                                     measurement=self.settings.influx_measurement,
                                     symbol=contract.symbol,
                                     expiry=str(contract.lastTradeDateOrContractMonth.split(" ")[0]),
                                     contractId=str(contract.conId),
                                     local_symbol=contract.localSymbol)

    def onBarUpdate(self, bars: BarDataList, hasNewBar: bool):
        if hasNewBar and bars.reqId in self.request_ids:
            new_bar = bars[-1]
            self.logger.info(f'{str(new_bar.time)} {bars.contract.symbol}')
            asyncio.get_event_loop().create_task(self.save_to_influx(new_bar, bars.contract))
            del bars[:]

    async def prepare_contracts(self):

        # Get the first contract in all the continuous futures of interest
        cont_fut = [el[0] for el in
                      await asyncio.gather(*[self.ib.qualifyContractsAsync(ContFuture(i, exchange=self.settings.exchange)) for i in
                                             self.settings.tickers])]

        # Get actual future contracts based on the continuous contracts
        fut = [el[0] for el in await asyncio.gather(*[self.ib.qualifyContractsAsync(Future(conId=c.conId)) for c in
                                                      cont_fut])]
        return fut

    async def get_live_futures(self):
        fut = await self.prepare_contracts()
        for contract in fut:
            self.request_ids.append(self.ib.client._reqIdSeq)
            self.ib.reqRealTimeBars(contract, self.settings.bar_size, whatToShow=self.settings.whatToShow, useRTH=False)
