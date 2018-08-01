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
from datetime import datetime
import plotly

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
test_data = b'\x84\xa3typ\xadblock_manager\xa5klass\xa9DataFrame\xa4axes\x92\x86\xa3typ\xa5index\xa5klass\xa5Index\xa4name\xc0\xa5dtype\xa6object\xa4data\x95\xa4open\xa4high\xa3low\xa5close\xa6volume\xa8compress\xc0\x88\xa3typ\xaedatetime_index\xa5klass\xadDatetimeIndex\xa4name\xa4date\xa5dtype\xb3datetime64[ns, UTC]\xa4data\xc7\xf8\x00\x00\x00\xb7{\xac\xb75\x15\x00\x00\x06\rA\x066\x15\x00\x00\xf3\xc0\xfe\xf16\x15\x00\x00BR\x93@7\x15\x00\x00\x91\xe3\'\x8f7\x15\x00\x00\xe0t\xbc\xdd7\x15\x00\x00/\x06Q,8\x15\x00\x00\x1c\xba\x0e\x189\x15\x00\x00kK\xa3f9\x15\x00\x00\xba\xdc7\xb59\x15\x00\x00\tn\xcc\x03:\x15\x00\x00X\xff`R:\x15\x00\x00E\xb3\x1e>;\x15\x00\x00\x94D\xb3\x8c;\x15\x00\x00\xe3\xd5G\xdb;\x15\x00\x002g\xdc)<\x15\x00\x00\x81\xf8px<\x15\x00\x00n\xac.d=\x15\x00\x00\xbd=\xc3\xb2=\x15\x00\x00[`\xecO>\x15\x00\x00\xaa\xf1\x80\x9e>\x15\x00\x00\x97\xa5>\x8a?\x15\x00\x00\xe66\xd3\xd8?\x15\x00\x005\xc8g\'@\x15\x00\x00\x84Y\xfcu@\x15\x00\x00\xd3\xea\x90\xc4@\x15\x00\x00\xc0\x9eN\xb0A\x15\x00\x00\x0f0\xe3\xfeA\x15\x00\x00^\xc1wMB\x15\x00\x00\xadR\x0c\x9cB\x15\x00\x00\xfc\xe3\xa0\xeaB\x15\xa4freq\xc0\xa2tz\xa3UTC\xa8compress\xc0\xa6blocks\x92\x86\xa4locs\x86\xa3typ\xa7ndarray\xa5shape\x91\x04\xa4ndim\x01\xa5dtype\xa5int64\xa4data\xc7 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\xa8compress\xc0\xa6values\xc8\x03\xe0\x00=\n\xd7\xa3p=\xf2?q=\n\xd7\xa3p\xf1?\x00\x00\x00\x00\x00\x00\xf4?\xf6(\\\x8f\xc2\xf5\xf4?\xb8\x1e\x85\xebQ\xb8\xfa?{\x14\xaeG\xe1z\x00@\xd7\xa3p=\n\xd7\xff?\xecQ\xb8\x1e\x85\xeb\xf9?{\x14\xaeG\xe1z\xf4?\xecQ\xb8\x1e\x85\xeb\xfd?\x8f\xc2\xf5(\\\x8f\x00@\x9a\x99\x99\x99\x99\x99\xf9?\xd7\xa3p=\n\xd7\xeb?\x85\xebQ\xb8\x1e\x85\xe3?ffffff\xe6?\xaeG\xe1z\x14\xae\xd7?\x85\xebQ\xb8\x1e\x85\xe3?\x00\x00\x00\x00\x00\x00\xe0?\xd7\xa3p=\n\xd7\xe3?\x9a\x99\x99\x99\x99\x99\xd9?\x9a\x99\x99\x99\x99\x99\xe1?q=\n\xd7\xa3p\xed?\x9a\x99\x99\x99\x99\x99\xf1?\\\x8f\xc2\xf5(\\\xe7?\x00\x00\x00\x00\x00\x00\xf0?\x1f\x85\xebQ\xb8\x1e\xfd?\xd7\xa3p=\n\xd7\xfb?\\\x8f\xc2\xf5(\\\xe7?333333\xff?\x00\x00\x00\x00\x00\x00\xf4?\x8f\xc2\xf5(\\\x8f\xf6?)\\\x8f\xc2\xf5(\xf4?\xc3\xf5(\\\x8f\xc2\xf1?q=\n\xd7\xa3p\xf5?{\x14\xaeG\xe1z\xf8?R\xb8\x1e\x85\xebQ\x00@\x9a\x99\x99\x99\x99\x99\x01@\xd7\xa3p=\n\xd7\xff?R\xb8\x1e\x85\xebQ\xfc?q=\n\xd7\xa3p\xf9?\xe1z\x14\xaeG\xe1\x02@\x8f\xc2\xf5(\\\x8f\x00@\x9a\x99\x99\x99\x99\x99\xf9?\xd7\xa3p=\n\xd7\xeb?\x00\x00\x00\x00\x00\x00\xe8?H\xe1z\x14\xaeG\xe9?\\\x8f\xc2\xf5(\\\xdf?\xcd\xcc\xcc\xcc\xcc\xcc\xe4?=\n\xd7\xa3p=\xe2?\xd7\xa3p=\n\xd7\xe3?\xe1z\x14\xaeG\xe1\xda?\x9a\x99\x99\x99\x99\x99\xe9?\xcd\xcc\xcc\xcc\xcc\xcc\xf0?\xd7\xa3p=\n\xd7\xf3?)\\\x8f\xc2\xf5(\xec?R\xb8\x1e\x85\xebQ\xfc?=\n\xd7\xa3p=\x00@\x9a\x99\x99\x99\x99\x99\xfd?\\\x8f\xc2\xf5(\\\x01@\xb8\x1e\x85\xebQ\xb8\x00@\x9a\x99\x99\x99\x99\x99\xf9?\xe1z\x14\xaeG\xe1\xfa?=\n\xd7\xa3p=\xf2?\x00\x00\x00\x00\x00\x00\xf0?\xaeG\xe1z\x14\xae\xf3?\xf6(\\\x8f\xc2\xf5\xf4?\xb8\x1e\x85\xebQ\xb8\xfa?R\xb8\x1e\x85\xebQ\x00@\xaeG\xe1z\x14\xae\xfb?\xf6(\\\x8f\xc2\xf5\xf4?\x00\x00\x00\x00\x00\x00\xf4?\xecQ\xb8\x1e\x85\xeb\xfd?\\\x8f\xc2\xf5(\\\xfb?\x1f\x85\xebQ\xb8\x1e\xf5?\x00\x00\x00\x00\x00\x00\xe0?\x85\xebQ\xb8\x1e\x85\xe3?\x85\xebQ\xb8\x1e\x85\xdb?\xaeG\xe1z\x14\xae\xd7?\x00\x00\x00\x00\x00\x00\xe0?\\\x8f\xc2\xf5(\\\xdf?\xe1z\x14\xaeG\xe1\xda?\x8f\xc2\xf5(\\\x8f\xd2?\xf6(\\\x8f\xc2\xf5\xe0?\x9a\x99\x99\x99\x99\x99\xe9?\x14\xaeG\xe1z\x14\xee?ffffff\xe6?\xc3\xf5(\\\x8f\xc2\xed?\x9a\x99\x99\x99\x99\x99\xf9?\\\x8f\xc2\xf5(\\\xef?\\\x8f\xc2\xf5(\\\xe7?\xaeG\xe1z\x14\xae\xf7?{\x14\xaeG\xe1z\xf0?\xc3\xf5(\\\x8f\xc2\xed?\xd7\xa3p=\n\xd7\xf3?\xc3\xf5(\\\x8f\xc2\xf1?q=\n\xd7\xa3p\xf5?\xaeG\xe1z\x14\xae\xf7?)\\\x8f\xc2\xf5(\xfc?\x9a\x99\x99\x99\x99\x99\x01@\x1f\x85\xebQ\xb8\x1e\xfd?\x85\xebQ\xb8\x1e\x85\xfb?\xcd\xcc\xcc\xcc\xcc\xcc\xf8?333333\x01@\xaeG\xe1z\x14\xae\xfb?ffffff\xf6?\x85\xebQ\xb8\x1e\x85\xe3?)\\\x8f\xc2\xf5(\xe4?\x85\xebQ\xb8\x1e\x85\xdb?\\\x8f\xc2\xf5(\\\xdf?R\xb8\x1e\x85\xebQ\xe0?=\n\xd7\xa3p=\xe2?\xe1z\x14\xaeG\xe1\xda?\xe1z\x14\xaeG\xe1\xda?\xf6(\\\x8f\xc2\xf5\xe8?\xcd\xcc\xcc\xcc\xcc\xcc\xf0?\xf6(\\\x8f\xc2\xf5\xf0?R\xb8\x1e\x85\xebQ\xe8?\x00\x00\x00\x00\x00\x00\xfc?R\xb8\x1e\x85\xebQ\xfc?)\\\x8f\xc2\xf5(\xf0?\x00\x00\x00\x00\x00\x00\x00@\x8f\xc2\xf5(\\\x8f\xfa?\x8f\xc2\xf5(\\\x8f\xf2?\xaeG\xe1z\x14\xae\xef?\xa5shape\x92\x04\x1f\xa5dtype\xa7float64\xa5klass\xaaFloatBlock\xa8compress\xc0\x86\xa4locs\x86\xa3typ\xa7ndarray\xa5shape\x91\x01\xa4ndim\x01\xa5dtype\xa5int64\xa4data\xd7\x00\x04\x00\x00\x00\x00\x00\x00\x00\xa8compress\xc0\xa6values\xc7\xf8\x00\x96\x13\x00\x00\x00\x00\x00\x00\x9f\x13\x00\x00\x00\x00\x00\x00\x1b\x00\x00\x00\x00\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x002\x00\x00\x00\x00\x00\x00\x00\x15\x00\x00\x00\x00\x00\x00\x00:\x00\x00\x00\x00\x00\x00\x00U\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x00\x00\x00\x00.\x00\x00\x00\x00\x00\x00\x00)\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\xc5Z\x00\x00\x00\x00\x00\x00S\x1f\x00\x00\x00\x00\x00\x00E\x00\x00\x00\x00\x00\x00\x00*\x00\x00\x00\x00\x00\x00\x00>\x00\x00\x00\x00\x00\x00\x006\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x00\x00\x00\x00\x0c\x02\x00\x00\x00\x00\x00\x00E\x08\x00\x00\x00\x00\x00\x00\x8a\x04\x00\x00\x00\x00\x00\x00\xbc.\x00\x00\x00\x00\x00\x00\xd8+\x00\x00\x00\x00\x00\x00\xa3\x08\x00\x00\x00\x00\x00\x00\x0c.\x00\x00\x00\x00\x00\x00\xd8/\x00\x00\x00\x00\x00\x00N\x0e\x00\x00\x00\x00\x00\x00\x08\x13\x00\x00\x00\x00\x00\x00\x19 \x00\x00\x00\x00\x00\x00\x1c"\x00\x00\x00\x00\x00\x00\xa5shape\x92\x01\x1f\xa5dtype\xa5int64\xa5klass\xa8IntBlock\xa8compress\xc0'


def get_data(right, symbol, expiry_date, strike):
    query = 'select t.*  ' \
            'from contracts c left join option_ticks t on c."conId" = t."conId" ' \
            'where c.strike = {} and c.symbol=\'{}\' and c.right=\'{}\' and  c."lastTradeDateOrContractMonth"::date = \'{}\' ' \
            'order by t.time '.format(strike, symbol, right, expiry_date)

    my_data = pd.read_sql(query, connection, parse_dates={"time": {"utc": True}})
    #my_data.price = my_data.price.astype(float)
    #my_data['size'] = my_data['size'].astype(int)
    tick_data = my_data.groupby(by='time').agg({"size":["sum"],"price":"mean"})
    return tick_data



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
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars Candlestick', href='/apps/daily_bars_candle',
                                 className='nav-item nav-link btn active btn-outline-success'),
                      ]),
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
                      html.Div([
                          html.Label("Strike:", htmlFor="strike", className='form-check-label'),
                          dcc.Dropdown(
                              id='ticks-strike',
                              className='form-check-inline form-check-input',
                              clearable=False,
                              value='180'
                          ),

                      ], className='form-check-inline col-2'),

                      dcc.Graph(
                          style={'height': 500},
                          id='ticks-graph',
                          className='col',
                      ),
                      dcc.Interval(
                          id='interval-component',
                          interval=600 * 1000,  # in milliseconds
                          n_intervals=0
                      ),
                  ])


def get_points(my_data, symbol, right, selected_date, strike):
    has_marker = {'color': f'{has_color}', 'size': 5, 'symbol': 'square'}
    hasnt_marker = {'color': f'{hasnt_color}', 'size': 5, 'symbol': 'square'}

    trace = [
        go.Scatter(
            x=my_data.index,
            y=[x.item() for x in my_data['price'].values],
            xaxis='x1',
            yaxis='y1',
            mode='lines',
            line=dict(
                shape='spline'
            )

        ),
        go.Bar(
            x=my_data.index,
            y=[x.item() for x in my_data['size'].values],
            name='Daily Volume',
            xaxis='x1',
            yaxis='y2',
            marker=dict(
                color='rgb(158,202,225)',
                line=dict(
                    color='rgb(8,48,107)',
                    width=1.5,
                )
            ),
            opacity=0.6
        ),

    ]

    return {
        'data': trace,
    }


@app.callback(Output('ticks-strike', 'options'),
              [Input('right', 'value'),
               Input('symbol', 'value'),
               Input('interval-component', 'n_intervals'),
               Input('date-picker', 'date')])
def get_strikes(right, symbol, n_intervals, date):
    query = 'select distinct c.strike, t."conId" is not null as has_ticks ' \
            'from contracts c left join option_ticks t on c."conId" = t."conId" ' \
            'where c.symbol=\'{}\' and c.right=\'{}\' and  c."lastTradeDateOrContractMonth"::date = \'{}\' ' \
            'group by c.strike, t."conId" ' \
            'order by c.strike ASC '.format(symbol, right, date)

    strikes = pd.read_sql(query, connection)
    strikes.strike = strikes.strike.astype(float)

    options_list = []

    for index, row in strikes.iterrows():
        rec = {'label': row.strike, 'value': row.strike}

        if row.has_ticks is not True:
            rec['disabled'] = 'True'

        options_list.append(rec)

    return options_list


@app.callback(Output('ticks-graph', 'figure'),
              [Input('right', 'value'),
               Input('symbol', 'value'),
               Input('interval-component', 'n_intervals'),
               Input('date-picker', 'date'),
               Input('ticks-strike', 'value')])
def update_bars_figure(right, symbol, n_intervals, date, strike):
    fig = plotly.tools.make_subplots(rows=3, cols=1, specs=[[{'rowspan': 2}], [{}], [{}]], vertical_spacing=0.01,
                                     shared_xaxes=True,
                                     shared_yaxes=False)
    layout = fig['layout']
    my_data = get_data(right, symbol, date, strike)

    if isinstance(my_data, pd.DataFrame) and not my_data.empty:
        my_points = get_points(my_data, symbol, right, dt.strptime(date.split(" ")[0], '%Y-%M-%d'), strike)
        date_string = dt.strptime(date.split(" ")[0], '%Y-%M-%d').strftime("%Y%M%d")

        layout['title'] = f'Ticks - {date_string} {symbol} {strike} {right} '
        layout['showlegend'] = True
        layout['legend'] = go.Legend(
            x=0,
            y=1.0
        )
        layout['margin'] = go.Margin(l=100, r=40, t=40, b=30)

        fig['layout'] = layout

        fig.append_trace(my_points['data'][0], 1, 1)
        fig.append_trace(my_points['data'][1], 3, 1)
    else:
        layout['annotations'] = [dict(xref='paper', x=0.05, y=0,
                                      xanchor='middle', yanchor='middle',
                                      text='No data found for given parameters',
                                      font=dict(family='Arial', size=40),
                                      showarrow=False)]

    return fig
