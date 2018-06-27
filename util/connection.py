import os

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.pool import NullPool
from aioinflux import InfluxDBClient
#from influxdb import DataFrameClient

DB_URL = os.environ.get('DB_URL')

if not DB_URL:
    raise ValueError("DB_URL not present in the environment")

Base = automap_base()
engine = create_engine(DB_URL, poolclass=NullPool)
Base.prepare(engine, reflect=True)

meta = MetaData()
meta.reflect(bind=engine)

contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]
contract_table = meta.tables["contracts"]
contract_daily_bar_table = meta.tables["contract_daily_bars"]
equity_contract_table = meta.tables["equity_contracts"]

session_factory = sessionmaker(bind=engine, expire_on_commit=False)
Session = scoped_session(session_factory)
Base.query = Session.query_property()

session = Session()

influx_client = InfluxDBClient(database='stocks')

