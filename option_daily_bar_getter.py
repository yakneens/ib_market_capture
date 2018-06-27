import datetime

import pandas as pd
from ib_insync import *
from sqlalchemy import update, TIMESTAMP
import asyncio
from util import connection as db
import util.logging as my_log
import logging

OPTION_DAILY_BAR_GETTER = 2

class OptionDailyBarGetter:

    def __init__(self, ib, requests):
        self.logger = my_log.SetupLogger("get_option_daily_bars")
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib
        self.daily_bars_sema = asyncio.Semaphore(1)
        self.requests = requests

    @staticmethod
    def set_cant_get_daily_bars_flag(conId):
        result = db.engine.execute(db.contract_table.update().where(db.contract_table.c.conId == conId).values(cantGetDailyBars=True))


    @staticmethod
    def to_df(my_bars, conId):
        bar_df = util.df(my_bars)
        bar_df['date'] = bar_df['date'].astype(pd.Timestamp)
        bar_df = bar_df.loc[lambda df: df.volume > 0, :]
        bar_df['conId'] = conId
        bar_df['addedOn'] = datetime.datetime.now()
        return bar_df

    @staticmethod
    def save_to_db(bars, conId):
        bars.to_sql("contract_daily_bars", db.engine, if_exists="append", index=False, dtype={'date': TIMESTAMP(timezone=True)})
        result = db.engine.execute(db.contract_table.update().where(db.contract_table.c.conId == conId).values(
            daily_bar_load_date=datetime.datetime.now()))

    @staticmethod
    async def save_to_influx(bars, contract):
        bars = bars.set_index('date')
        return await db.influx_client.write(bars,
                                   measurement='option_daily_bars',
                                   symbol=contract.symbol,
                                   expiry=str(contract.lastTradeDateOrContractMonth.split(" ")[0]),
                                   contractId=str(contract.conId),
                                   strike=str(contract.strike),
                                   right=contract.right,
                                   local_symbol=contract.localSymbol)

    @staticmethod
    def get_daily_bar_df():
        query = 'select * from contracts c join contract_ib_first_timestamp t on c."conId" = t."contractId" ' \
                'where t."firstTimestamp" is not null and c.daily_bar_load_date is null and c.expired is not true ' \
                ' and c."cantGetDailyBars" is not true ' \
                'order by "lastTradeDateOrContractMonth" ASC, c.priority '
        con_df = pd.read_sql(query, db.engine)
        return con_df

    async def get_daily_bars(self):

        con_df = self.get_daily_bar_df()
        num_rows = len(con_df)

        for index, row in con_df.iterrows():
            my_con = Option(conId=row.conId, exchange=row.exchange)

            num_days = (datetime.datetime.now(datetime.timezone.utc) - row.firstTimestamp).days + 1

            #print(f"{index}/{num_rows} {datetime.datetime.now()} Processing contract {row.localSymbol} {row.lastTradeDateOrContractMonth} {row.firstTimestamp}")
            self.logger.info(f"{index}/{num_rows} {datetime.datetime.now()} Processing contract {row.localSymbol} {row.lastTradeDateOrContractMonth} {row.firstTimestamp}")
            try:
                async with self.daily_bars_sema:
                    self.requests[self.ib.client._reqIdSeq] = OPTION_DAILY_BAR_GETTER
                    my_bars = await self.ib.reqHistoricalDataAsync(my_con, endDateTime='', durationStr='{} D'.format(num_days),
                                               barSizeSetting='8 hours', whatToShow='TRADES', useRTH=False, formatDate=2)
            except ValueError as e:
                self.logger.error("Error getting historic bars for {} {}".format(row.localSymbol, e))
                #print("Error getting historic bars for {} {}".format(row.localSymbol, e))
                await asyncio.sleep(5)
                continue

            if my_bars:
                bar_df = self.to_df(my_bars, row.conId)
                self.logger.info("Saving to DB")
                #print("Saving to DB")
                self.save_to_db(bar_df, row.conId)
                self.logger.info("Saving to Influx")
                #print("Saving to Influx")

                try:
                    await self.save_to_influx(bar_df, row)
                except ValueError as e:
                    self.logger.error(f" {e} Couldn't save to Influx")
                    #print(f" {e} Couldn't save to Influx")
                await asyncio.sleep(5)
