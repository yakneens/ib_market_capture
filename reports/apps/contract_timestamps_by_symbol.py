import plotly.graph_objs as go
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import datetime
import time
from dash.dependencies import Output, Input
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData
from datetime import timedelta
from app import app
from dateutil.relativedelta import relativedelta, FR
from datetime import datetime as dt

engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]
contracts = meta.tables["contracts"]

has_color = 'rgba(76, 175, 80,1.0)'
hasnt_color = 'rgba(255, 193, 7,1.0)'
cant_color = 'rgba(156, 39, 176,1.0)'


def get_timestamp_data(selected_date):
    query = 'select count(*), c.symbol, c."cantGetFirstTimestamp", ' \
            't."firstTimestamp" is not null AS "hasTimestamp" ' \
            'from contracts c left join contract_ib_first_timestamp t on c."conId" = t."contractId" ' \
            'where c."lastTradeDateOrContractMonth"::date = \'{}\' ' \
            'group by c.symbol, c."cantGetFirstTimestamp", "hasTimestamp" ' \
            'order by c.symbol, "hasTimestamp"'.format(selected_date)

    con_df = pd.read_sql(query, connection)
    con_df.loc[con_df['hasTimestamp'].isna(), 'hasTimestamp'] = False
    con_df.loc[con_df['cantGetFirstTimestamp'].isna(), 'cantGetFirstTimestamp'] = False
    return con_df


layout = html.Div(className='container',
                  children=[
                      html.Nav(className='navbar navbar-expand-lg navbar-light bg-light nav-tabs nav-fill', children=[
                          html.A('Timestamps By Date', href='/apps/contract_timestamps',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Timestamps By Symbol', href='/apps/contract_timestamps_by_symbol',
                                 className='nav-item nav-link btn btn-outline-success active'),
                          html.A('Timestamps By Date and Symbol', href='/apps/contract_timestamps_by_date_and_symbol',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars', href='/apps/daily_bars',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars By Date', href='/apps/daily_bars_by_date',
                                 className='nav-item nav-link btn  btn-outline-success'),
                          html.A('Daily Bars By Date And Symbol', href='/apps/daily_bars_by_date_and_symbol',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars By Symbol And Strike', href='/apps/daily_bars_by_symbol_and_strike',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars Candlestick', href='/apps/daily_bars_candle',
                                 className='nav-item nav-link btn btn-outline-success'),
                      ]),
                      html.Div([
                          html.Label("Expiry Date:", htmlFor="date-picker", className='form-check-label'),
                          html.Div([
                              dcc.DatePickerSingle(
                                  id='date-picker',
                                  min_date_allowed=dt(2018, 6, 15),
                                  max_date_allowed=dt(2030, 12, 31),
                                  initial_visible_month=dt.now(),
                                  date=dt.now() + relativedelta(weekday=FR(+1)),
                              ),

                          ], className='form-check'),
                      ], className='form-check-inline col-auto'),

                      dcc.Graph(
                          style={'height': 300, },
                          id='my-timestamp-graph'
                      ),
                      dcc.Interval(
                          id='interval-component',
                          interval=600 * 1000,  # in milliseconds
                          n_intervals=0
                      )

                  ])


def get_timestamp_bars(my_data, selected_date):
    trace = [
        go.Bar(
            x=my_data.query('hasTimestamp == True')[
                'symbol'],
            y=my_data.query('hasTimestamp == True')['count'],
            name='Has Timestamp',
            marker=go.Marker(color=has_color),
            # , line=dict(color='rgb(0, 0, 0)',width=1)
        ),
        go.Bar(
            x=my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True')[
                'symbol'],
            y=my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True')['count'],
            name='No Timestamp',
            marker=go.Marker(color=hasnt_color),
        ),

        go.Bar(
            x=my_data.query('cantGetFirstTimestamp == True')['symbol'],
            y=my_data.query('cantGetFirstTimestamp == True')['count'],
            name='Can\'t Get Timestamp',
            marker=go.Marker(color=cant_color),
        ),
    ]

    return {
        'data': trace,
        'layout': go.Layout(
            title=f'Timestamps by symbol - {selected_date.strftime("%d %b %Y")}',
            showlegend=False,
            legend=go.Legend(
                x=0,
                y=1.0
            ),
            margin=go.Margin(l=40, r=40, t=40, b=30),
            barmode='relative',
            barnorm='percent'
        )
    }


@app.callback(Output('my-timestamp-graph', 'figure'), [Input('date-picker', 'date'),
                                                       Input('interval-component', 'n_intervals')])
def update_timestamp_figure(date, n_intervals):
    con_df = get_timestamp_data(date)
    return get_timestamp_bars(con_df, dt.strptime(date.split(" ")[0], '%Y-%m-%d'))
