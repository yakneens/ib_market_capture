import datetime

import pandas as pd

from ib_insync import *
from sqlalchemy import update
import logging

from util import connection as db

import asyncio
import util.logging as my_log

OPTION_TICK_GETTER = 3


class OptionTickGetter:

    def __init__(self, ib, requests):
        self.logger = my_log.SetupLogger("get_option_ticks")
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib
        self.requests = requests
        self.option_tick_sema = asyncio.Semaphore(1)

    query_template = 'select c.*, b."dailyBarId", b."date", b.volume from contracts c ' \
                     'join contract_daily_bars b on c."conId" = b."conId" ' \
                     'where {} and b.ticks_retrieved IS NULL and c.expired is not TRUE and ' \
                     'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now()) < {}  and ' \
                     'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now())  >= -2 ' \
                     'order by c."lastTradeDateOrContractMonth", c.priority, b.volume desc;'

    days_to_expiry_cutoff = '10'

    priorities = [(1, ' b.volume > 1000 '),
                  (2, ' b.volume <= 1000 and b.volume > 500 '),
                  (3, ' b.volume <= 500 and b.volume > 100 '),
                  (4, ' b.volume <= 100 ')]

    def get_daily_bars(self, priority):
        query = self.query_template.format(priority, self.days_to_expiry_cutoff)
        return pd.read_sql(query, db.engine)

    def update_ticks_retrieved(self, dailyBarId):
        stmt = update(db.contract_daily_bar_table).where(db.contract_daily_bar_table.c.dailyBarId == dailyBarId). \
            values(ticks_retrieved=True)
        db.engine.execute(stmt)

    async def write_to_influx(self, tick_df, contract):
        tick_df = tick_df.set_index('time')
        return await db.influx_client.write(tick_df,
                                            measurement='test',
                                            symbol=contract.symbol,
                                            expiry=str(contract.lastTradeDateOrContractMonth.split(" ")[0]),
                                            contractId=str(contract.conId),
                                            strike=str(contract.strike),
                                            right=contract.right,
                                            local_symbol=contract.localSymbol)

    async def get_ticks(self):
        for (priority_number, priority_sub) in self.priorities:
            #print.(f"Processing priority {priority_number}")
            self.logger.info(f"Processing priority {priority_number}")

            con_df = self.get_daily_bars(priority_sub)

            num_rows = len(con_df)

            for index, row in con_df.iterrows():

                #print.(f"Processing contract {index}/{num_rows} {row.localSymbol} for {row.date} with volume {row.volume}")
                self.logger.info(f"Processing contract {index}/{num_rows}  {row.localSymbol} for {row.date} with volume {row.volume}")
                cur_date = row.date.replace(hour=0, minute=0, second=0)

                if isinstance(cur_date, pd.Timestamp):
                    cur_date = cur_date.to_pydatetime()

                tickList = []

                this_contract = Option(conId=row.conId, exchange=row.exchange)

                while True:
                    try:
                        async with self.option_tick_sema:
                            self.requests[self.ib.client._reqIdSeq] = OPTION_TICK_GETTER
                            ticks = await self.ib.reqHistoricalTicksAsync(this_contract, cur_date, None, 1000, 'TRADES',
                                                                          useRth=False)
                    except:
                        self.logger.info("Couldn't get ticks")
                        #print.("Couldn't get ticks")
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
                            #print.("Writing to Influx")
                            self.logger.info("Writing to Influx")
                            await self.write_to_influx(tick_df, row)

                            self.logger.info("Writing to DB")
                            #print.("Writing to DB")
                            # self.update_ticks_retrieved(row.dailyBarId)
                        else:
                            #print.("No ticks found")
                            self.logger.info("No ticks found")
                    else:
                        self.logger.info("Allticks was empty")
                        #print.("Allticks was empty")

                else:
                    self.logger.info("No ticks found")
                    #print.("No ticks found")
