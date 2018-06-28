import datetime

import pandas as pd

from ib_insync import *
from sqlalchemy import update
import logging

from util import connection as db

import asyncio
import util.logging as my_log

from collections import namedtuple

import time

OptionTickGetterSettings = namedtuple('OptionTickGetterSettings',
                                     'what_to_show, influx_measurement, log_filename, days_to_expiry_cutoff, priorities,restart_period')

class OptionTickGetter:

    def __init__(self, ib: IB, settings: OptionTickGetterSettings):
        self.settings = settings
        self.logger = my_log.SetupLogger(self.settings.log_filename)
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib
        self.request_ids = []
        self.option_tick_sema = asyncio.Semaphore(1)

    query_template = 'select c.*, b."dailyBarId", b."date", b.volume from contracts c ' \
                     'join contract_daily_bars b on c."conId" = b."conId" ' \
                     'where {} and b.ticks_retrieved IS NULL and c.expired is not TRUE and ' \
                     'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now()) < {}  and ' \
                     'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now())  >= -2 ' \
                     'order by c."lastTradeDateOrContractMonth", c.priority, b.volume desc;'



    def get_daily_bars(self, priority):
        query = self.query_template.format(priority, self.settings.days_to_expiry_cutoff)
        return pd.read_sql(query, db.engine)

    def update_ticks_retrieved(self, dailyBarId):
        stmt = update(db.contract_daily_bar_table).where(db.contract_daily_bar_table.c.dailyBarId == dailyBarId). \
            values(ticks_retrieved=True)
        db.engine.execute(stmt)

    async def write_to_influx(self, tick_df, contract):
        tick_df = tick_df.set_index('time')
        return await db.influx_client.write(tick_df,
                                            measurement=self.settings.influx_measurement,
                                            symbol=contract.symbol,
                                            expiry=str(contract.lastTradeDateOrContractMonth.split(" ")[0]),
                                            contractId=str(contract.conId),
                                            strike=str(contract.strike),
                                            right=contract.right,
                                            local_symbol=contract.localSymbol)

    async def get_ticks(self):
        while True:
            start_time = time.time()
            for (priority_number, priority_sub) in self.settings.priorities:
                if time.time() - start_time > self.settings.restart_period:
                    self.logger.info("Timer elapsed, restarting.")
                    break

                self.logger.info(f"Processing priority {priority_number}")

                con_df = self.get_daily_bars(priority_sub)

                num_rows = len(con_df)

                for index, row in con_df.iterrows():

                    self.logger.info(f"Processing contract {index}/{num_rows}  {row.localSymbol} for {row.date} with volume {row.volume}")
                    cur_date = row.date.replace(hour=0, minute=0, second=0)

                    if isinstance(cur_date, pd.Timestamp):
                        cur_date = cur_date.to_pydatetime()

                    tickList = []

                    this_contract = Option(conId=row.conId, exchange=row.exchange)

                    while True:
                        try:
                            async with self.option_tick_sema:
                                self.request_ids.append(self.ib.client._reqIdSeq)
                                ticks = await self.ib.reqHistoricalTicksAsync(this_contract, cur_date, None, 1000, 'TRADES',
                                                                              useRth=False)
                        except:
                            self.logger.info("Couldn't get ticks")
                            raise
                        tickList.append(ticks)

                        if len(ticks) >= 1000:
                            cur_date = ticks[-1].time + datetime.timedelta(seconds=1)
                        else:
                            break

                    if len(tickList) > 0:
                        allTicks = [t for ticks in tickList for t in ticks]
                        if allTicks:
                            tick_df = util.df(allTicks)

                            if not tick_df.empty:
                                self.logger.info("Writing to Influx")
                                await self.write_to_influx(tick_df, row)

                                self.logger.info("Writing to DB")
                               #TODO - Uncomment this after testing.
                               #self.update_ticks_retrieved(row.dailyBarId)
                            else:
                                self.logger.info("No ticks found")
                        else:
                            self.logger.info("Allticks was empty")
                    else:
                        self.logger.info("No ticks found")

