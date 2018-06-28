import datetime
import pandas as pd
from ib_insync import *
import util.connection as db
from sqlalchemy import TIMESTAMP
import asyncio
import logging
import util.logging as my_log

from collections import namedtuple

OptionBarUpdaterSettings = namedtuple('OptionBarUpdaterSettings',
                                     'bar_size, what_to_show, db_table, influx_measurement, cant_get_bars_col, load_date_col, log_filename')


class OptionDailyBarUpdater:
    def __init__(self, ib: IB, settings: OptionBarUpdaterSettings):

        self.settings = settings

        self.logger = my_log.SetupLogger(self.settings.log_filename)
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib
        self.request_ids = []
        self.option_daily_bar_update_sema = asyncio.Semaphore(1)


    @staticmethod
    def to_df(my_bars, conId):
        bar_df = util.df(my_bars)
        bar_df['date'] = bar_df['date'].apply(lambda d: d.to_pydatetime())
        bar_df = bar_df.loc[lambda df: df.volume > 0, :]
        bar_df['conId'] = conId
        bar_df['addedOn'] = datetime.datetime.now()
        return bar_df

    def filter_existing(self, my_bars, conId):
        query = f'select date from {self.settings.db_table} where "conId" = {conId}'
        existing_dates = pd.read_sql(query, db.engine)['date']
        return my_bars.query('date not in @existing_dates')

    def save_to_db(self, bars, conId):
        bars['date'] = bars['date'].astype(pd.Timestamp)
        bars.to_sql(self.settings.db_table, db.engine, if_exists="append", index=False, dtype={'date': TIMESTAMP(timezone=True)})

    def update_load_date(self, conId):
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

    def get_updateable_contracts(self):
        query = 'select c2.*, b."lastDate" from contracts c2 join (select max(date) as "lastDate", b."conId"' \
                'from {} b join contracts c on b."conId" = c."conId"' \
                'where DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now()) < 10 and ' \
                'c.expired is not true and ' \
                'DATE_PART(\'day\', now() - c."{}" :: timestamp with time zone) > 0 ' \
                'group by b."conId") b on c2."conId" = b."conId" order by c2."lastTradeDateOrContractMonth", c2.priority;'.format(self.settings.db_table, self.settings.load_date_col)

        return pd.read_sql(query, db.engine)

    async def update_daily_bars(self):
        con_df = self.get_updateable_contracts()
        num_rows = len(con_df)
        tasks = []

        for index, row in con_df.iterrows():
            my_con = Option(conId=row.conId, exchange="SMART")

            try:
                await self.ib.qualifyContractsAsync(my_con)
            except ValueError:
                continue

            num_days = (datetime.datetime.now(datetime.timezone.utc) - row.lastDate).days + 1
            #print(f"{index}/{num_rows} {datetime.datetime.now()} Processing contract {row.localSymbol} {row.lastTradeDateOrContractMonth} {row.lastDate}")
            self.logger.info(f"{index}/{num_rows} {datetime.datetime.now()} Processing contract {row.localSymbol} {row.lastTradeDateOrContractMonth} {row.lastDate}")
            try:
                async with self.option_daily_bar_update_sema:
                    self.request_ids.append(self.ib.client._reqIdSeq)
                    my_bars = await self.ib.reqHistoricalDataAsync(my_con, endDateTime='', durationStr='{} D'.format(num_days),
                                               barSizeSetting=self.settings.bar_size, whatToShow=self.settings.what_to_show, useRTH=False, formatDate=2)
            except ValueError as e:
                self.logger.error("Error getting historic bars for {} {}".format(row.localSymbol, e))
                #print("Error getting historic bars for {} {}".format(row.localSymbol, e))
                continue

            if my_bars:
                bar_df = self.to_df(my_bars, row.conId)
                bar_df = self.filter_existing(bar_df, row.conId)

                if not bar_df.empty:
                    self.logger.info("Saving to DB")
                    #print("Saving to DB")
                    self.save_to_db(bar_df, row.conId)
                    self.logger.info("Saving to Influx")
                    #print("Saving to Influx")
                    await self.save_to_influx(bar_df, row)
                else:
                    self.logger.info("No updates")
                    #print("No updates")

                    self.update_load_date(row.conId)
