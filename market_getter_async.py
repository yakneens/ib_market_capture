import datetime
import time

import pandas as pd
from ib_insync import *
from sqlalchemy import update
from util import connection as db
from first_timestamp_getter import FirstTimestampGetter, FirstTimestampSettings
from option_daily_bar_getter import OptionDailyBarGetter, OptionBarGetterSettings
from option_tick_getter import OptionTickGetter, OptionTickGetterSettings
from option_daily_bar_updater import OptionDailyBarUpdater, OptionBarUpdaterSettings
from historical_equity_bar_getter import *
from live_futures_getter import LiveFuturesGetter, LiveFuturesSettings
import os
import random
import asyncio


def init_ib():
    ib = IB()
    ib.errorEvent += onError
    IB_PORT = os.environ.get("IB_PORT")
    ib.connect('127.0.0.1', IB_PORT, clientId=1337)
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
        # main()


def get_first_timestamp_settings():
    return FirstTimestampSettings(log_filename="get_option_first_timestamp")


def get_option_bar_getter_settings():
    return OptionBarGetterSettings(bar_size="8 hours",
                                   what_to_show="TRADES",
                                   db_table="contract_daily_bars_test",
                                   influx_measurement="contract_daily_bars_test",
                                   log_filename="get_option_daily_bars",
                                   cant_get_bars_col="cantGetDailyBars",
                                   load_date_col="daily_bar_load_date")


def get_option_tick_getter_settings():
    return OptionTickGetterSettings(what_to_show="TRADES",
                                    log_filename="get_option_ticks",
                                    influx_measurement="option_trades_test",
                                    days_to_expiry_cutoff=10,
                                    restart_period=30,
                                    priorities=[(1, ' b.volume > 1000 '),
                                                (2, ' b.volume <= 1000 and b.volume > 500 '),
                                                (3, ' b.volume <= 500 and b.volume > 100 '),
                                                (4, ' b.volume <= 100 ')])


def get_option_bar_updater_settings():
    return OptionBarUpdaterSettings(bar_size="8 hours",
                                    what_to_show="TRADES",
                                    db_table="contract_daily_bars",
                                    influx_measurement="contract_daily_bars",
                                    log_filename="get_option_daily_bars",
                                    cant_get_bars_col="cantGetDailyBars",
                                    load_date_col="daily_bar_load_date")


def get_historical_3_minute_settings():
    return HistoricalEquityBarGetterSettings(db_table='stock_3_min_bars_test',
                                             influx_measurement='stock_3_min_bars_test',
                                             lookback_period='20 D',
                                             bar_size='3 mins',
                                             skip_list=["SPY", "QQQ", "EEM", "XLF", "GLD", "EFA", "IWM",
                                                        "VXX", "FXI", "USO", "XOP",
                                                        "HYG", "AAPL", "BAC", "MU", "FB", "BABA", "NVDA",
                                                        "AMD", "GE", "TSLA", "NFLX", "AMZN", "MSFT", "SNAP", "T"],
                                             update_colname='threeMinuteBarsLoadedOn',
                                             retry_offsets=[RetryOffset(10, datetime.timedelta(minutes=5)),
                                                            RetryOffset(10, datetime.timedelta(hours=1)),
                                                            RetryOffset(10, datetime.timedelta(days=1))],
                                             log_filename='get_historical_3_minute_bars',
                                             sleep_duration=0)


def get_historical_1_second_settings():
    return HistoricalEquityBarGetterSettings(db_table='stock_1_sec_bars_test',
                                             influx_measurement='stock_1_sec_bars_test',
                                             lookback_period='2000 S',
                                             bar_size='1 secs',
                                             skip_list=[],
                                             update_colname='oneSecBarsLoadedOn',
                                             retry_offsets=[RetryOffset(10, datetime.timedelta(minutes=1)),
                                                            RetryOffset(10, datetime.timedelta(hours=1))],
                                             log_filename='get_historical_1_second_bars',
                                             sleep_duration=5)


def get_live_futures_globex_settings():
    return LiveFuturesSettings(tickers=["ES", "NQ", "RTY", "NKD", "LE", "HE", "GF"],
                               exchange="GLOBEX",
                               bar_size=5,
                               whatToShow="TRADES",
                               influx_measurement="futures_5_sec_bars_test",
                               log_filename="get_live_futures")


def get_live_futures_nymex_settings():
    return LiveFuturesSettings(tickers=["CL", "RB", "NG", "GC", "SI"],
                               exchange="NYMEX",
                               bar_size=5,
                               whatToShow="TRADES",
                               influx_measurement="futures_5_sec_bars_test",
                               log_filename="get_live_futures")


def get_live_futures_cfe_settings():
    return LiveFuturesSettings(tickers=["VIX"],
                               exchange="CFE",
                               bar_size=5,
                               whatToShow="TRADES",
                               influx_measurement="futures_5_sec_bars_test",
                               log_filename="get_live_futures")


def get_live_futures_ecbot_settings():
    return LiveFuturesSettings(tickers=["ZS", "ZL", "ZB", "ZF", "YM"],
                               exchange="ECBOT",
                               bar_size=5,
                               whatToShow="TRADES",
                               influx_measurement="futures_5_sec_bars_test",
                               log_filename="get_live_futures")


async def my_main(ib):
    try:
        tasks = [HistoricalEquityBarGetter(ib, get_historical_3_minute_settings()).get_historical_equity_bars(),
                 OptionTickGetter(ib, get_option_tick_getter_settings()).get_ticks(),
                 OptionDailyBarGetter(ib, get_option_bar_getter_settings()).get_daily_bars(),
                 OptionDailyBarUpdater(ib, get_option_bar_updater_settings()).update_daily_bars(),
                 FirstTimestampGetter(ib, get_first_timestamp_settings()).get_first_trade_date(),
                 LiveFuturesGetter(ib, get_live_futures_globex_settings()).get_live_futures(),
                 LiveFuturesGetter(ib, get_live_futures_ecbot_settings()).get_live_futures(),
                 LiveFuturesGetter(ib, get_live_futures_nymex_settings()).get_live_futures(),
                 LiveFuturesGetter(ib, get_live_futures_cfe_settings()).get_live_futures(),
                 HistoricalEquityBarGetter(ib, get_historical_1_second_settings()).get_historical_equity_bars()]
        await asyncio.gather(*tasks)

    except ValueError as e:
        print(f"Arrived here {e}")


if __name__ == '__main__':
    start_time = time.time()
    ib = init_ib()
    asyncio.ensure_future(my_main(ib, ))
    IB.run()
    print("Execution time was: {}".format(str(time.time() - start_time)))
