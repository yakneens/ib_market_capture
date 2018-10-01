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
                             'log_filename, cant_get_timestamp, start_cutoff, end_cutoff, last_load, date_order')

timeout_retry_flag = 0

class FirstTimestampGetter:
    def __init__(self, ib, settings: FirstTimestampSettings):
        self.settings = settings
        self.logger = my_log.SetupLogger(self.settings.log_filename)
        self.logger.setLevel(logging.INFO)
        self.logger.info("now is %s", datetime.datetime.now())

        self.ib = ib
        self.first_trade_date_sema = asyncio.Semaphore(1)
        self.request_ids = []

        if not self.onError in self.ib.barUpdateEvent:
            self.ib.errorEvent.connect(self.onError, weakRef=False)

    def onError(self, reqId, errorCode, errorString, contract):
        if errorCode == 200 and errorString == 'No security definition has been found for the request':
            this_contract = db.engine.query(db.contract_table).filter_by(conId=contract.conId).first()
            if this_contract:
                expiry = datetime.datetime.strptime(this_contract.lastTradeDateOrContractMonth.split(" ")[0], '%Y%m%d')
                if expiry < datetime.datetime.now():
                    print("Contract expired, setting expiry flag")
                    stmt = update(db.contract_table).where(db.contract_table.c.conId == contract.conId).values(expired=True)
                    db.engine.execute(stmt)
        if errorCode == 162:
            global timeout_retry_flag
            if timeout_retry_flag >= 5:
                print("Request timed out. Setting flag.")
                self.set_timeout_flag(True, contract.conId)
                timeout_retry_flag = 0
            else:
                timeout_retry_flag += 1
                print(f"Timeout try {timeout_retry_flag}")
        elif errorCode == 1102:
            print("Restarting after outage")


    def set_timeout_flag(self, flag_value: bool, conId):
        stmt = update(db.contract_table). \
            where(db.contract_table.c.conId == conId). \
            values(timestampReqTimedout=flag_value,
                   timestampLoadAttemptDate=datetime.datetime.now(datetime.timezone.utc))
        db.engine.execute(stmt)


    @staticmethod
    def set_cant_get_timestamp_flag(conId):
        stmt = update(db.contract_table).where(db.contract_table.c.conId == conId).values(
            cantGetFirstTimestamp=True)
        db.engine.execute(stmt)

    async def get_timestamp(self, contract):

        if contract.secType == "OPT":
            my_con = Option(conId=contract.conId, exchange=contract.exchange)
        elif contract.secType == "FOP":
            my_con = FuturesOption(conId=contract.conId, exchange=contract.exchange)
        else:
            self.logger.error(f"Unknown security type {contract.secType}")
            exit(1)
        global timeout_retry_flag
        while True:
            try:
                self.request_ids.append(self.ib.client._reqIdSeq)
                first_date = await self.ib.reqHeadTimeStampAsync(contract=my_con, whatToShow="TRADES", useRTH=False, formatDate=2)
            except ValueError as e:
                if str(e) == "time data '-9223372036854775' does not match format '%Y%m%d  %H:%M:%S'":
                    self.logger.info("Can't get timestamp. Setting flag.")
                    self.set_cant_get_timestamp_flag(contract.conId)
                    self.set_timeout_flag(False, contract.conId)
                timeout_retry_flag = 0
                raise e

            self.logger.info(f"Completed {contract.conId} {str(first_date)}")
            #print(f"Completed {contract.conId} {str(first_date)}")

            if not isinstance(first_date, datetime.datetime):
                if timeout_retry_flag == 0 or timeout_retry_flag >= 5:
                    timeout_retry_flag = 0
                    raise ValueError
                self.ib.sleep(10)
            else:
                timeout_retry_flag = 0
                break

        return contract.conId, first_date

    def get_first_trade_date_df(self, date_to_get):
        query = 'SELECT * FROM contracts c ' \
                'WHERE c."conId" NOT IN (SELECT DISTINCT "contractId" ' \
                'FROM contract_ib_first_timestamp ' \
                'WHERE "contractId" IS NOT NULL ' \
                'AND "firstTimestamp" IS NOT NULL) ' \
                'AND symbol <> \'GLW\' ' \
                'AND expired IS NOT TRUE AND "cantGetFirstTimestamp" IS NOT TRUE ' \
                'order by c."lastTradeDateOrContractMonth" ASC, priority '

        query = 'SELECT c."conId", c."exchange", c."localSymbol", c."secType", c."lastTradeDateOrContractMonth" FROM contracts c ' \
                'WHERE c."conId" NOT IN (SELECT DISTINCT "contractId" ' \
                'FROM contract_ib_first_timestamp ' \
                'WHERE "contractId" IS NOT NULL ' \
                'AND "firstTimestamp" IS NOT NULL) ' \
                'AND c."timestampReqTimedout" is not TRUE ' \
                'AND expired IS NOT TRUE AND "cantGetFirstTimestamp" IS {} and ' \
                'c."lastTradeDateOrContractMonth" :: date = \'{}\' and ' \
                '(c."timestampLoadAttemptDate" is null or DATE_PART(\'day\', now() -  c."timestampLoadAttemptDate" :: timestamp with time zone) >= {}) ' \
                'ORDER BY c.priority ASC, c."timestampReqTimedout" '.format(self.settings.cant_get_timestamp,
                                                                            date_to_get,
                                                                            self.settings.last_load)

        con_df = pd.read_sql(query, db.engine)
        return con_df

    def get_trade_dates_df(self):
        # start_cutoff = 0
        # end_cutoff = 60
        # last_load = 2
        # cant_get_timestamp = "true"
        # date_order = "ASC"

        date_query = 'SELECT distinct c."lastTradeDateOrContractMonth"::date FROM contracts c ' \
                     'WHERE c."conId" NOT IN (SELECT DISTINCT "contractId" ' \
                     'FROM contract_ib_first_timestamp ' \
                     'WHERE "contractId" IS NOT NULL ' \
                     'AND "firstTimestamp" IS NOT NULL) ' \
                     'AND c."timestampReqTimedout" is not TRUE ' \
                     'AND expired IS NOT TRUE AND "cantGetFirstTimestamp" IS {} and ' \
                     'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now())  >= {} and ' \
                     'DATE_PART(\'day\', c."lastTradeDateOrContractMonth" :: timestamp with time zone - now())  <= {} and ' \
                     '(c."timestampLoadAttemptDate" is null or ' \
                     'DATE_PART(\'day\', now() -  c."timestampLoadAttemptDate" :: timestamp with time zone) >= {}) ' \
                     'ORDER BY c."lastTradeDateOrContractMonth"::date {} '. \
            format(self.settings.cant_get_timestamp,
                   self.settings.start_cutoff,
                   self.settings.end_cutoff,
                   self.settings.last_load,
                   self.settings.date_order)

        dates_df = pd.read_sql(date_query, db.engine)
        return dates_df


    @staticmethod
    def save_new_first_date(contract, first_date):
        db.engine.execute(db.contract_timestamp_table.insert().values(contractId=contract.conId, firstTimestamp=first_date,
                                                     addedOn=datetime.datetime.now()))

    async def get_first_trade_date(self,):

        dates_df = self.get_trade_dates_df()

        for index, row in dates_df.iterrows():

            con_df = self.get_first_trade_date_df(row.lastTradeDateOrContractMonth)
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
