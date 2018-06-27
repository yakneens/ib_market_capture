import datetime
import time

import pandas as pd
from ib_insync import *
from sqlalchemy import TIMESTAMP
import util.connection as db
import util.logging as my_log
import logging
import asyncio
from collections import namedtuple


class HistoricalEquityBarGetter:
    BarSettings = namedtuple('BarSettings',
                             'db_table, influx_measurement, lookback_period, bar_size, skip_list, update_colname, retry_offsets, log_filename, sleep_duration')
    RetryOffset = namedtuple('RetryOffest', 'num_retries, offset')

    bar_settings_lookup = {
        '3_minutes': BarSettings(db_table='stock_3_min_bars', influx_measurement='stock_3_min_bars',
                                 lookback_period='20 D', bar_size='3 mins',
                                 skip_list=["SPY", "QQQ", "EEM", "XLF", "GLD", "EFA", "IWM", "VXX", "FXI", "USO", "XOP",
                                            "HYG", "AAPL", "BAC", "MU"],
                                 update_colname='threeMinuteBarsLoadedOn',
                                 retry_offsets=[RetryOffset(10, datetime.timedelta(minutes=5)),
                                                RetryOffset(10, datetime.timedelta(hours=1)),
                                                RetryOffset(10, datetime.timedelta(days=1))],
                                 log_filename='get_historical_3_minute_bars',
                                 sleep_duration=0),
        '1_second': BarSettings(db_table='stock_1_sec_bars', influx_measurement='stock_1_sec_bars',
                                lookback_period='2000 S', bar_size='1 secs', skip_list=[],
                                update_colname='oneSecBarsLoadedOn',
                                retry_offsets=[RetryOffset(10, datetime.timedelta(minutes=1)),
                                               RetryOffset(10, datetime.timedelta(hours=1))],
                                log_filename='get_historical_1_second_bars',
                                sleep_duration=5)}

    def __init__(self, ib, requests, bar_settings_key):
        self.bar_settings = self.bar_settings_lookup[bar_settings_key]

        self.logger = my_log.SetupLogger(self.bar_settings.log_filename)
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib
        self.requests = requests

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
            bars.to_sql(self.bar_settings.db_table, db.engine, if_exists="append", index=False,
                        dtype={'date': TIMESTAMP(timezone=True)})
        else:
            print("Data frame was empty.")

        result = db.engine.execute(
            db.equity_contract_table.update().where(db.equity_contract_table.c.conId == conId).values(
                {self.bar_settings.update_colname: datetime.datetime.now()}))

    async def save_to_influx(self, bars, contract):
        if not bars.empty:
            bars = bars.set_index('date')
            return await db.influx_client.write(bars,
                                      measurement=self.bar_settings.influx_measurement,
                                      symbol=contract.symbol,
                                      contractId=str(contract.conId))

    def get_historical_equity_contracts(self):
        db_table_name = self.bar_settings.db_table
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
        retry_offsets = self.bar_settings.retry_offsets
        offsets_long = []
        for this_offset in retry_offsets:
            offsets_long += this_offset.num_retries * [this_offset.offset]
        return offsets_long

    async def get_historical_equity_bars(self):
        con_df = self.get_historical_equity_contracts()

        for index, row in con_df.iterrows():

            if row.symbol in self.bar_settings.skip_list:
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
                        bars = await self.ib.reqHistoricalDataAsync(
                            my_con,
                            endDateTime=dt,
                            durationStr=self.bar_settings.lookback_period,
                            barSizeSetting=self.bar_settings.bar_size,
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
                        self.bar_settings.skip_list.appen(row.symbol)
                        break

                dt = bars[0].date.strftime('%Y%m%d %H:%M:%S')
                self.logger.info(dt)

                bar_df = self.to_df(bars, row.conId, row.symbol, row.equityContractId)

                self.logger.info("Saving to DB")
                self.save_to_db(bar_df, row.conId)

                self.logger.info("Saving to Influx")
                await self.save_to_influx(bar_df, row)

                self.logger.info("Execution time was: {}".format(str(time.time() - start_time)))

                await asyncio.sleep(self.bar_settings.sleep_duration)

