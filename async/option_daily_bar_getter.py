import datetime

import pandas as pd
from ib_insync import *
from sqlalchemy import update, TIMESTAMP
import asyncio
from util import connection as db
import util.logging as my_log
import logging
from collections import namedtuple

OptionBarGetterSettings = namedtuple('OptionBarGetterSettings',
                                     'bar_size, what_to_show, db_table, influx_measurement, cant_get_bars_col, load_date_col, log_filename')


class OptionDailyBarGetter:

    def __init__(self, ib, settings: OptionBarGetterSettings):
        self.settings = settings
        self.logger = my_log.SetupLogger(self.settings.log_filename)
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib
        self.daily_bars_sema = asyncio.Semaphore(1)
        self.request_ids = []

        self.ib.errorEvent.connect(self.onError, weakRef=False)

    def set_cant_get_bars_flag(self, conId):
        result = db.engine.execute(db.contract_table.update().where(db.contract_table.c.conId == conId).values(
            {self.settings.cant_get_bars_col: True}))

    def onError(self, reqId, errorCode, errorString, contract):
        if reqId in self.request_ids and errorCode == 162:
            self.logger.info(f"Couldn't get data for {contract.conId}, setting cant_get_bars flag.")
            self.set_cant_get_bars_flag(contract.conId)

    @staticmethod
    def to_df(my_bars, conId):
        bar_df = util.df(my_bars)
        bar_df['date'] = bar_df['date'].astype(pd.Timestamp)
        bar_df = bar_df.loc[lambda df: df.volume > 0, :]
        bar_df['conId'] = conId
        bar_df['addedOn'] = datetime.datetime.now()
        return bar_df

    def save_to_db(self, bars, conId):
        bars.to_sql(self.settings.db_table, db.engine, if_exists="append", index=False,
                    dtype={'date': TIMESTAMP(timezone=True)})
        result = db.engine.execute(db.contract_table.update().where(db.contract_table.c.conId == conId).values(
            {self.settings.load_date_col: datetime.datetime.now()}))

    async def save_to_influx(self, bars, contract):
        bars = bars.set_index('date')
        return await db.influx_client.write(bars,
                                            measurement=self.settings.influx_measurement,
                                            symbol=contract.symbol,
                                            expiry=str(contract.lastTradeDateOrContractMonth.split(" ")[0]),
                                            contractId=str(contract.conId),
                                            strike=str(contract.strike),
                                            right=contract.right,
                                            local_symbol=contract.localSymbol)

    def get_daily_bar_df(self):
        query = 'select * from contracts c join contract_ib_first_timestamp t on c."conId" = t."contractId" ' \
                'where t."firstTimestamp" is not null and c.{} is null and c.expired is not true ' \
                ' and c."{}" is not true ' \
                'order by "lastTradeDateOrContractMonth" ASC, c.priority '.format(self.settings.load_date_col, self.settings.cant_get_bars_col)
        con_df = pd.read_sql(query, db.engine)
        return con_df

    async def get_daily_bars(self):

        con_df = self.get_daily_bar_df()
        num_rows = len(con_df)

        for index, row in con_df.iterrows():
            my_con = Option(conId=row.conId, exchange=row.exchange)

            num_days = (datetime.datetime.now(datetime.timezone.utc) - row.firstTimestamp).days + 1


            self.logger.info(
                f"{index}/{num_rows} {datetime.datetime.now()} Processing contract {row.localSymbol} {row.lastTradeDateOrContractMonth} {row.firstTimestamp}")
            try:
                async with self.daily_bars_sema:
                    self.request_ids.append(self.ib.client._reqIdSeq)
                    my_bars = await self.ib.reqHistoricalDataAsync(my_con, endDateTime='',
                                                                   durationStr='{} D'.format(num_days),
                                                                   barSizeSetting=self.settings.bar_size, whatToShow=self.settings.what_to_show,
                                                                   useRTH=False, formatDate=2)
            except ValueError as e:
                self.logger.error("Error getting historic bars for {} {}".format(row.localSymbol, e))
                # print("Error getting historic bars for {} {}".format(row.localSymbol, e))
                await asyncio.sleep(5)
                continue

            if my_bars:
                bar_df = self.to_df(my_bars, row.conId)
                self.logger.info("Saving to DB")
                self.save_to_db(bar_df, row.conId)

                self.logger.info("Saving to Influx")

                try:
                    await self.save_to_influx(bar_df, row)
                except ValueError as e:
                    self.logger.error(f" {e} Couldn't save to Influx")
                await asyncio.sleep(5)
