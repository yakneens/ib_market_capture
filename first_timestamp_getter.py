import datetime

import asyncio
import pandas as pd
from ib_insync import *
from sqlalchemy import update
from util import connection as db
import util.logging as my_log
import logging
from collections import namedtuple


FirstTimestampSettings = namedtuple('FirstTimestampSettings',
                             'log_filename,')

class FirstTimestampGetter:
    def __init__(self, ib, settings: FirstTimestampSettings):
        self.settings = settings
        self.logger = my_log.SetupLogger(self.settings.log_filename)
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.contract_data = self.get_first_trade_date_df()
        self.ib = ib
        self.first_trade_date_sema = asyncio.Semaphore(1)
        self.request_ids = []

    @staticmethod
    def set_cant_get_timestamp_flag(conId):
        stmt = update(db.contract_table).where(db.contract_table.c.conId == conId).values(
            cantGetFirstTimestamp=True)
        db.engine.execute(stmt)

    async def get_timestamp(self, contract):

        my_con = Option(conId=contract.conId, exchange=contract.exchange)
        try:
            self.request_ids.append(self.ib.client._reqIdSeq)
            first_date = await self.ib.reqHeadTimeStampAsync(contract=my_con, whatToShow="TRADES", useRTH=False, formatDate=2)
        except ValueError as e:
            if str(e) == "time data '-9223372036854775' does not match format '%Y%m%d  %H:%M:%S'":
                self.logger.info("Can't get timestamp. Setting flag.")
                self.set_cant_get_timestamp_flag(contract.conId)
            raise e

        self.logger.info(f"Completed {contract.conId} {str(first_date)}")
        #print(f"Completed {contract.conId} {str(first_date)}")

        if not isinstance(first_date, datetime.datetime):
            raise ValueError

        return contract.conId, first_date

    @staticmethod
    def get_first_trade_date_df():
        query = 'SELECT * FROM contracts c ' \
                'WHERE c."conId" NOT IN (SELECT DISTINCT "contractId" ' \
                'FROM contract_ib_first_timestamp ' \
                'WHERE "contractId" IS NOT NULL ' \
                'AND "firstTimestamp" IS NOT NULL) ' \
                'AND symbol <> \'GLW\' ' \
                'AND expired IS NOT TRUE AND "cantGetFirstTimestamp" IS NOT TRUE ' \
                'order by c."lastTradeDateOrContractMonth" ASC, priority '

        con_df = pd.read_sql(query, db.engine)
        return con_df

    @staticmethod
    def save_new_first_date(contract, first_date):
        db.engine.execute(db.contract_timestamp_table.insert().values(contractId=contract.conId, firstTimestamp=first_date,
                                                     addedOn=datetime.datetime.now()))

    async def get_first_trade_date(self,):
        con_df = self.contract_data
        #print(len(con_df))
        num_rows = len(con_df)
        self.logger.info(len(con_df))
        for index, row in con_df.iterrows():
            self.logger.info(f'{index}/{num_rows} {row.localSymbol}')
            try:
                async with self.first_trade_date_sema:
                    (conId, first_date) = await self.get_timestamp(row)

                self.save_new_first_date(row, first_date)
            except ValueError as e:
                #print(e)
                self.logger.error(e)
                continue
