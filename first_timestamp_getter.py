import datetime

import asyncio
import pandas as pd
from ib_insync import *
from sqlalchemy import update
from util import connection as db

FIRST_TIMESTAMP_GETTER = 1

class FirstTimestampGetter:
    def __init__(self, ib, requests):
        self.contract_data = self.get_first_trade_date_df()
        self.contract_timestamp_table = db.meta.tables["contract_ib_first_timestamp"]
        self.contracts_table = db.meta.tables["contracts"]
        self.ib = ib
        self.first_trade_date_sema = asyncio.Semaphore(1)
        self.requests = requests

    async def get_timestamp(self, contract):

        my_con = Option(conId=contract.conId, exchange=contract.exchange)
        try:
            self.requests[self.ib.client._reqIdSeq] = FIRST_TIMESTAMP_GETTER
            first_date = await self.ib.reqHeadTimeStampAsync(contract=my_con, whatToShow="TRADES", useRTH=False, formatDate=2)
        except ValueError as e:
            if str(e) == "time data '-9223372036854775' does not match format '%Y%m%d  %H:%M:%S'":
                stmt = update(self.contracts_table).where(self.contracts_table.c.conId == contract.conId).values(cantGetFirstTimestamp=True)
                db.engine.execute(stmt)
            raise e

        print(f"Completed {contract.conId} {str(first_date)}")

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

    def save_new_first_date(self, contract, first_date):
        db.engine.execute(self.contract_timestamp_table.insert().values(contractId=contract.conId, firstTimestamp=first_date,
                                                     addedOn=datetime.datetime.now()))

    async def get_first_trade_date(self,):
        con_df = self.contract_data
        print(len(con_df))

        for index, row in con_df.iterrows():
            try:
                async with self.first_trade_date_sema:
                    (conId, first_date) = await self.get_timestamp(row)

                self.save_new_first_date(row, first_date)
            except ValueError as e:
                print(e)
                continue
