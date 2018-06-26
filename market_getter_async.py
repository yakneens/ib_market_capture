import datetime
import time

import pandas as pd
from ib_insync import *
from sqlalchemy import update
from util import connection as db
from first_timestamp_getter import FirstTimestampGetter
from option_daily_bar_getter import OptionDailyBarGetter
from option_tick_getter import OptionTickGetter
import os
import random
import asyncio

FIRST_TIMESTAMP_GETTER = 1
OPTION_DAILY_BAR_GETTER = 2
OPTION_TICK_GETTER = 3

requests = {}

def init_ib():
    ib = IB()
    ib.errorEvent += onError
    IB_PORT = os.environ.get("IB_PORT")
    ib.connect('127.0.0.1', IB_PORT, clientId=int(random.random() * 100))
    return ib

def onError(reqId, errorCode, errorString, contract):
    print("ERROR", reqId, errorCode, errorString)
    if errorCode == 200 and errorString == 'No security definition has been found for the request':
        contracts = db.meta.tables["contracts"]
        this_contract = db.session.query(contracts).filter_by(conId=contract.conId).first()
        if this_contract:
            expiry = datetime.datetime.strptime(this_contract.lastTradeDateOrContractMonth.split(" ")[0], '%Y%m%d')
            if expiry < datetime.datetime.now():
                print("Contract expired, setting expiry flag")
                stmt = update(contracts).where(contracts.c.conId == contract.conId).values(expired=True)
                db.engine.execute(stmt)
    elif errorCode == 1102:
        print("Restarting after outage")
        #main()
    elif errorCode == 162:
        req_type = requests.get(reqId)
        if req_type == OPTION_DAILY_BAR_GETTER:
            print(f"Couldn't get data for {contract.conId}, setting cantGetDailyBars flag.")
            OptionDailyBarGetter.set_cant_get_daily_bars_flag(contract.conId)

async def my_main(ib, ):
    print('here')
    try:
        await asyncio.gather(*[FirstTimestampGetter(ib, requests).get_first_trade_date(),OptionDailyBarGetter(ib, requests).get_daily_bars(),OptionTickGetter(ib, requests).get_ticks()])
    except ValueError:
        print("Arrived here")


if __name__ == '__main__':
    start_time = time.time()
    ib = init_ib()
    asyncio.ensure_future(my_main(ib, ))
    IB.run()
    print("Execution time was: {}".format(str(time.time() - start_time)))
