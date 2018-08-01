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


def get_daily_bar_data(selected_date):
    query = 'select count(*), symbol, b."dailyBarId" is null as no_bars, c."cantGetDailyBars" ' \
            'from contracts c left join contract_daily_bars b on c."conId" = b."conId" ' \
            'left join contract_ib_first_timestamp t on c."conId" = t."contractId" ' \
            'where "lastTradeDateOrContractMonth"::date = \'{}\' ' \
            'AND t."firstTimestamp" is not null ' \
            'group by c.symbol, no_bars, c."cantGetDailyBars" ' \
            'order by c.symbol, no_bars, c."cantGetDailyBars" '.format(selected_date)

    con_df = pd.read_sql(query, connection)
    con_df.loc[con_df['cantGetDailyBars'].isna(), 'cantGetDailyBars'] = False

    return con_df


layout = html.Div(className='container',
                  children=[
                      html.Nav(className='navbar navbar-expand-lg navbar-light bg-light nav-tabs nav-fill', children=[
                          html.A('Timestamps By Date', href='/apps/contract_timestamps',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Timestamps By Symbol', href='/apps/contract_timestamps_by_symbol',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Timestamps By Date and Symbol', href='/apps/contract_timestamps_by_date_and_symbol',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars', href='/apps/daily_bars',
                                 className='nav-item nav-link btn btn-outline-success active'),
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
                          style={'height': 300},
                          id='my-daily-bar-graph'
                      ),
                      dcc.Interval(
                          id='interval-component',
                          interval=600 * 1000,  # in milliseconds
                          n_intervals=0
                      )

                  ])


def get_daily_bar_bars(my_data, selected_date):
    trace = [
        go.Bar(
            x=my_data.query('no_bars != True')[
                'symbol'],
            y=my_data.query('no_bars != True')['count'],
            name='Has Bars',
            marker=go.Marker(color=has_color),
        ),

        go.Bar(
            x=my_data.query('no_bars == True & cantGetDailyBars != True')[
                'symbol'],
            y=my_data.query('no_bars == True & cantGetDailyBars != True')['count'],
            name='No Bars',
            marker=go.Marker(color=hasnt_color),
        ),
        go.Bar(
            x=my_data.query('no_bars == True & cantGetDailyBars == True')['symbol'],
            y=my_data.query('no_bars == True & cantGetDailyBars == True')['count'],
            name='Can\'t Get Bars',
            marker=go.Marker(color=cant_color),
        ),
    ]

    return {
        'data': trace,
        'layout': go.Layout(
            title=f'Daily bars by symbol and date - {selected_date.strftime("%d %b %Y")}',
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


@app.callback(Output('my-daily-bar-graph', 'figure'), [Input('date-picker', 'date'),
                                                       Input('interval-component', 'n_intervals')])
def update_daily_bar_figure(date, n_intervals):
    con_df = get_daily_bar_data(date)
    return get_daily_bar_bars(con_df, dt.strptime(date.split(" ")[0], '%Y-%m-%d'))
