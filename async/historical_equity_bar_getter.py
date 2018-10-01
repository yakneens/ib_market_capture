import datetime
import time
from typing import Any

import pandas as pd
from ib_insync import *
from sqlalchemy import TIMESTAMP
import util.connection as db
import util.logging as my_log
import logging
import asyncio
from collections import namedtuple

HistoricalEquityBarGetterSettings = namedtuple('HistoricalEquityBarGetterSettings',
                             'db_table, influx_measurement, lookback_period, bar_size, skip_list, update_colname, retry_offsets, log_filename, sleep_duration')
RetryOffset = namedtuple('RetryOffset', 'num_retries, offset')

class HistoricalEquityBarGetter:

    def __init__(self, ib, settings):
        self.settings = settings

        self.logger = my_log.SetupLogger(self.settings.log_filename)
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib
        self.request_ids = []

        self.equity_historical_bar_sema = asyncio.Semaphore(1)

    @staticmethod
    def to_df(my_bars, conId, symbol, equityContractId):
        bar_df = util.df(my_bars)
        bar_df['date'] = bar_df['date'].astype(pd.Timestamp)

        if symbol == "VXX":
            bar_df = bar_df.loc[lambda df: df.barCount > 0, :]
        else:
            bar_df = bar_df.loc[lambda df: df.volume > 0, :]

        bar_df['conId'] = conId
        bar_df['symbol'] = symbol
        bar_df['equityContractId'] = equityContractId
        bar_df['addedOn'] = datetime.datetime.now()
        return bar_df

    def save_to_db(self, bars, conId):
        if not bars.empty:
            bars.to_sql(self.settings.db_table, db.engine, if_exists="append", index=False,
                        dtype={'date': TIMESTAMP(timezone=True)})
        else:
            print("Data frame was empty.")

        result = db.engine.execute(
            db.equity_contract_table.update().where(db.equity_contract_table.c.conId == conId).values(
                {self.settings.update_colname: datetime.datetime.now()}))

    async def save_to_influx(self, bars, contract):
        if not bars.empty:
            bars = bars.set_index('date')
            return await db.influx_client.write(bars,
                                      measurement=self.settings.influx_measurement,
                                      symbol=contract.symbol,
                                      contractId=str(contract.conId))

    def get_historical_equity_contracts(self):
        db_table_name = self.settings.db_table
        if db_table_name in db.meta.tables.keys():
            query = 'select e.symbol, e."conId", e."equityContractId", min(b.date) as date, priority ' \
                'from equity_contracts e left join ' \
                '{} b on e."equityContractId" = b."equityContractId" ' \
                'group by e.symbol, priority, e."equityContractId", e."conId" ' \
                'order by priority, e."equityContractId" '.format(db_table_name)
        else:
            query = 'select e.symbol, e."conId", e."equityContractId", priority from equity_contracts e order by priority, e."equityContractId"'
            self.logger.info(f"Table {db_table_name} doesn't exist. A new one will be created")

        return pd.read_sql(query, db.engine)

    def get_retry_offsets(self):
        retry_offsets = self.settings.retry_offsets
        offsets_long = []
        for this_offset in retry_offsets:
            offsets_long += this_offset.num_retries * [this_offset.offset]
        return offsets_long

    async def get_historical_equity_bars(self):
        con_df = self.get_historical_equity_contracts()

        for index, row in con_df.iterrows():

            if row.symbol in self.settings.skip_list:
                continue

            my_con = Stock(conId=row.conId, exchange="SMART")
            q_con = await self.ib.qualifyContractsAsync(my_con)

            self.logger.info(f"{datetime.datetime.now()} Processing contract {row.symbol}")

            if 'date' in con_df.columns and not pd.isnull(row.date):
                dt = row.date.astimezone(datetime.timezone.utc).strftime('%Y%m%d %H:%M:%S')
            else:
                dt = ''

            self.logger.info(f"{datetime.datetime.now()} Processing contract {row.symbol} with end date { dt }")

            fail_count = 0
            retry_offsets = self.get_retry_offsets()

            while True:
                start_time = time.time()
                try:
                    async with self.equity_historical_bar_sema:
                        self.request_ids.append(self.ib.client._reqIdSeq)
                        bars = await self.ib.reqHistoricalDataAsync(
                            my_con,
                            endDateTime=dt,
                            durationStr=self.settings.lookback_period,
                            barSizeSetting=self.settings.bar_size,
                            whatToShow='TRADES',
                            useRTH=False,
                            formatDate=2)


                except ValueError as e:
                    self.logger.error("Error getting historic bars for {} {}".format(row.symbol, e))
                    raise

                if not bars:
                    if fail_count < len(retry_offsets):
                        this_offset = retry_offsets[fail_count]
                        dt = (datetime.datetime.strptime(dt, '%Y%m%d %H:%M:%S') - this_offset).strftime(
                            '%Y%m%d %H:%M:%S')
                        fail_count += 1
                        self.logger.info(f"Got no bars, trying {dt}. Attempt {fail_count+1}")
                        await asyncio.sleep(5)
                        continue
                    else:
                        self.logger.info(f"No more bars. Adding {row.symbol} to skip list.")
                        self.settings.skip_list.appen(row.symbol)
                        break

                dt = bars[0].date.strftime('%Y%m%d %H:%M:%S')
                self.logger.info(dt)

                bar_df = self.to_df(bars, row.conId, row.symbol, row.equityContractId)

                self.logger.info("Saving to DB")
                self.save_to_db(bar_df, row.conId)

                self.logger.info("Saving to Influx")
                await self.save_to_influx(bar_df, row)

                self.logger.info("Execution time was: {}".format(str(time.time() - start_time)))

                await asyncio.sleep(self.settings.sleep_duration)



