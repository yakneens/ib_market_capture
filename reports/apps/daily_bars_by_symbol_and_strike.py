import plotly.graph_objs as go
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import datetime
import time
from dash.dependencies import Output, Input
from sqlalchemy import create_engine
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
no_timestamp_color = 'rgba(255, 87, 34,1.0)'


def get_points(my_data, symbol, right, selected_date):
    has_marker = {'color': f'{has_color}', 'size': 5, 'symbol': 'square'}
    hasnt_marker = {'color': f'{hasnt_color}', 'size': 5, 'symbol': 'square'}

    has = my_data.query('has_bars == True')
    hasnt = my_data.query('has_bars != True')

    trace = [
        go.Scatter(
            x=has.date,
            y=has.strike,
            name='Has bars',
            marker=has_marker,
            mode='markers'
        ),
        go.Scatter(
            x=hasnt.date,
            y=hasnt.strike,
            name='Has no bars',
            marker=hasnt_marker,
            mode='markers'
        ),
    ]

    return {
        'data': trace,
        'layout': go.Layout(
            title=f'Daily bars - {selected_date.strftime("%Y%M%d")} {symbol} {right} ',
            showlegend=True,
            legend=go.Legend(
                x=0,
                y=1.0
            ),
            margin=go.Margin(l=100, r=40, t=40, b=30),
            xaxis={
                "title": 'Date',
                "autorange": True,
                "showticklabels": True,
                "automargin": True,
                "tickformat": "%d-%b-%y"
            },
            yaxis={
                "title": 'Strike',
                "autorange": True,
                "showticklabels": True,
                "automargin": True,
            }
        )
    }


def get_heat(my_data, symbol, right):
    has_marker = {'color': f'{has_color}', 'size': 5, 'symbol': 'square'}
    hasnt_marker = {'color': f'{hasnt_color}', 'size': 5, 'symbol': 'square'}
    cant_marker = {'color': f'{cant_color}', 'size': 5, 'symbol': 'square'}

    my_data['z'] = np.select(
        [my_data.hasTimestamp == True,
         (my_data.cantGetFirstTimestamp != True) & (my_data.hasTimestamp != True),
         my_data.cantGetFirstTimestamp == True],
        [0, 0.5, 1], np.nan)

    has = my_data.query('hasTimestamp == True')
    hasnt = my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True')
    cant = my_data.query('cantGetFirstTimestamp == True')

    trace = [
        go.Heatmap(
            x=my_data.strike,
            y=my_data.lastTradeDateOrContractMonth,
            z=my_data.z,
            colorscale=[[0, has_color], [0.5, hasnt_color], [1, cant_color]],
            zauto=False,
            zmax=1,
            zmin=0

        )
    ]
    return {
        'data': trace,
        'layout': go.Layout(
            title=f'Timestamps by expiry date - {symbol} {right}',
            showlegend=True,
            legend=go.Legend(
                x=0,
                y=1.0
            ),
            margin=go.Margin(l=100, r=40, t=40, b=30),
            yaxis={
                "type": 'category',
                "title": 'Expiry Date',
                "autorange": True,
                "showticklabels": True,
                "automargin": True,
                "tickformat": "%d-%b-%y"
            },

        )
    }


def get_data(right, symbol, expiry_date):
    query = 'select c.right, c.strike, ' \
            'b."dailyBarId" is not null as has_bars, b.date ' \
            'from contracts c left join contract_daily_bars b on c."conId" = b."conId" ' \
            'where c.symbol=\'{}\' and c.right=\'{}\' and  c."lastTradeDateOrContractMonth"::date = \'{}\' ' \
            'order by c.strike '.format(symbol, right, expiry_date)

    con_df = pd.read_sql(query, connection, parse_dates={"date": {"utc": True}})
    con_df.loc[con_df['has_bars'].isna(), 'has_bars'] = False
    con_df.strike = con_df.strike.astype(float)
    return con_df


def get_symbols():
    query = 'select distinct symbol from contracts order by symbol'

    symbols = pd.read_sql(query, connection)

    return [{'label': symbol, 'value': symbol} for symbol in symbols.symbol]


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
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars By Date', href='/apps/daily_bars_by_date',
                                 className='nav-item nav-link btn  btn-outline-success'),
                          html.A('Daily Bars By Date And Symbol', href='/apps/daily_bars_by_date_and_symbol',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars By Symbol And Strike', href='/apps/daily_bars_by_symbol_and_strike',
                                 className='nav-item nav-link btn active btn-outline-success'),
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
                      html.Div([
                          html.Label("Symbol:", htmlFor="symbol", className='form-check-label'),
                          dcc.Dropdown(
                              id='symbol',
                              options=get_symbols(),
                              value='QQQ',
                              className='form-check-inline form-check-input',
                              clearable=False
                          ),

                      ], className='form-check-inline col-2'),
                      html.Div([
                          html.Label("Right:", htmlFor='right', className='form-check-label'),
                          dcc.RadioItems(
                              id='right',
                              options=[
                                  {'label': 'Calls', 'value': 'C'},
                                  {'label': 'Puts', 'value': 'P'},
                              ],
                              value='C',
                              labelClassName='radio-inline',
                              inputClassName='radio form-check-input',
                              className=''
                          )

                      ], className='form-check-inline col-auto'),

                      dcc.Graph(
                          style={'height': 500},
                          id='my-bars-by-symbol-and-strike-graph',
                          className='col'
                      ),
                      dcc.Interval(
                          id='interval-component',
                          interval=600 * 1000,  # in milliseconds
                          n_intervals=0
                      ),

                  ])


@app.callback(Output('my-bars-by-symbol-and-strike-graph', 'figure'),
              [Input('right', 'value'),
               Input('symbol', 'value'),
               Input('interval-component', 'n_intervals'),
               Input('date-picker', 'date')])
def update_bars_figure(right, symbol, n_intervals, date):
    con_df = get_data(right, symbol, date)
    return get_points(con_df, symbol, right, dt.strptime(date.split(" ")[0], '%Y-%M-%d'))
