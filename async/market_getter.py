import datetime
import time

import pandas as pd
from ib_insync import *
from sqlalchemy import update
from util import connection as db
import os
import random
import threading
import asyncio

def init_ib():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    ib = IB()
    ib.errorEvent += onError
    IB_PORT = os.environ.get("IB_PORT")
    ib.connect('127.0.0.1', IB_PORT, clientId=int(random.random() * 100))
    return ib

def onError(reqId, errorCode, errorString, contract):
    print("ERROR", reqId, errorCode, errorString)
    if errorCode == 200 and errorString == 'No security definition has been found for the request':
        contracts = db.meta.tables["contracts"]
        this_contract = db.engine.query(contracts).filter_by(conId=contract.conId).first()
        if this_contract:
            expiry = datetime.datetime.strptime(this_contract.lastTradeDateOrContractMonth.split(" ")[0], '%Y%m%d')
            if expiry < datetime.datetime.now():
                print("Contract expired, setting expiry flag")
                stmt = update(contracts).where(contracts.c.conId == contract.conId).values(expired=True)
                db.engine.execute(stmt)
    elif errorCode == 1102:
        print("Restarting after outage")
        #main()

def get_timestamp(contract, ib, contracts):

    my_con = Option(conId=contract.conId, exchange=contract.exchange)
    try:
        first_date = ib.reqHeadTimeStamp(my_con, "TRADES", False, 2)
    except ValueError as e:
        if str(e) == "time data '-9223372036854775' does not match format '%Y%m%d  %H:%M:%S'":
            stmt = update(contracts).where(contracts.c.conId == contract.conId).values(cantGetFirstTimestamp=True)
            db.engine.execute(stmt)
        raise e

    print(f"Completed {contract.conId} {str(first_date)}")

    if not isinstance(first_date, datetime.datetime):
        raise ValueError

    return (contract.conId, first_date)

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


def get_first_trade_date():
    contract_timestamp_table = db.meta.tables["contract_ib_first_timestamp"]
    contracts = db.meta.tables["contracts"]

    con_df = get_first_trade_date()
    print(len(con_df))

    ib = init_ib()

    for index, row in con_df.iterrows():
        try:
            (conId, first_date) = get_timestamp(row, ib, contracts)
            result = db.engine.execute(
                contract_timestamp_table.insert().values(contractId=row.conId, firstTimestamp=first_date,
                                                         addedOn=datetime.datetime.now()))
        except ValueError as e:
            print(e)
            continue



def main():
    t = threading.Thread(target=get_first_trade_date)
    t.start()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("Execution time was: {}".format(str(time.time() - start_time)))
