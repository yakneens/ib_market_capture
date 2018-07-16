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

engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]
contracts = meta.tables["contracts"]

has_color = 'rgba(76, 175, 80,1.0)'
hasnt_color = 'rgba(255, 193, 7,1.0)'
cant_color = 'rgba(156, 39, 176,1.0)'


def unixTimeMillis(dt):
    return int(time.mktime(dt.timetuple()))


def unixToDatetime(unix):
    return pd.to_datetime(unix, unit='s')


def get_dates():
    query = 'select distinct "lastTradeDateOrContractMonth"::date from contracts order by "lastTradeDateOrContractMonth"'
    date_df = pd.read_sql(query, connection, parse_dates=["lastTradeDateOrContractMonth"]).query(
        'lastTradeDateOrContractMonth < 2019')
    return date_df.lastTradeDateOrContractMonth


def getMarks():
    my_dates = get_dates()
    result = {}
    next_friday = datetime.datetime.now().date() + relativedelta(weekday=FR(+1))
    selected_index = 0
    for i, date in my_dates.iteritems():
        result[i] = {"label": str(date.strftime('%d-%b-%y')),
                     "style": {"transform": "rotate(90deg)",
                               "white-space": "nowrap",
                               "margin-top": "2%"}}
        if date.date() == next_friday:
            selected_index = i

    return (selected_index, result)


def get_points(my_data):
    # has_c = my_data.query('hasTimestamp == True & right == "C"')
    # hasnt_c = my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True & right == "C"')
    # cant_c = my_data.query('cantGetFirstTimestamp == True & right == "C"')
    #

    has_marker = {'color': f'{has_color}', 'size': 5, 'symbol': 'square'}
    hasnt_marker = {'color': f'{hasnt_color}', 'size': 5, 'symbol': 'square'}
    cant_marker = {'color': f'{cant_color}', 'size': 5, 'symbol': 'square'}

    my_data['marker'] = np.select(
        [my_data.hasTimestamp == True,
         (my_data.cantGetFirstTimestamp != True) & (my_data.hasTimestamp != True),
         my_data.cantGetFirstTimestamp == True],
        [has_marker, hasnt_marker, cant_marker], np.nan)


    has = my_data.query('hasTimestamp == True')
    hasnt = my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True')
    cant = my_data.query('cantGetFirstTimestamp == True')

    trace = [
        go.Scatter(
            x=has.strike,
            y=has.lastTradeDateOrContractMonth,
            name='Has',
            marker=has_marker,
            mode='markers'
        ),
        go.Scatter(
            x=hasnt.strike,
            y=hasnt.lastTradeDateOrContractMonth,
            name='Hasn\'t',
            marker=hasnt_marker,
            mode='markers'
        ),
        go.Scatter(
            x=cant.strike,
            y=cant.lastTradeDateOrContractMonth,
            name='Can\'t',
            marker=cant_marker,
            mode='markers'
        ),

        # go.Bar(
        #     x=my_data.query('hasTimestamp == True')[
        #         'expiryDate'],
        #     y=my_data.query('hasTimestamp == True')['count'],
        #     name='Has Timestamp',
        #     marker=go.Marker(color=has_color),
        # ),
        # go.Bar(
        #     x=my_data.query('cantGetFirstTimestamp == True')['expiryDate'],
        #     y=my_data.query('cantGetFirstTimestamp == True')['count'],
        #     name='Can\'t Get Timestamp',
        #     marker=go.Marker(color=cant_color),
        # ),
    ]

    return {
        'data': trace,
        'layout': go.Layout(
            title='Count of timestamps by expiry date',
            showlegend=True,
            legend=go.Legend(
                x=0,
                y=1.0
            ),
            margin=go.Margin(l=100, r=40, t=40, b=30),
            yaxis={
                "type": 'category',
                "title": 'Expiry Date'}
        )
    }


def get_data(right, symbol):
    query = 'select c.right, c.strike, c."cantGetFirstTimestamp", c."lastTradeDateOrContractMonth"::date, ' \
            't."firstTimestamp" is not null AS "hasTimestamp" ' \
            'from contracts c left join contract_ib_first_timestamp t on c."conId" = t."contractId" ' \
            'where c.symbol=\'{}\' and c.right=\'{}\'  ' \
            'order by c."lastTradeDateOrContractMonth"::date, c.right, c.strike '.format(symbol, right)

    # 'where c.symbol = \'SPY\' and c."lastTradeDateOrContractMonth"::date in (\'20180713\', \'20180716\',\'20180720\') ' \

    con_df = pd.read_sql(query, connection, parse_dates=["lastTradeDateOrContractMonth"])
    # con_df['expiryDate'] = con_df['lastTradeDateOrContractMonth'].apply(
    #     lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
    con_df.loc[con_df['cantGetFirstTimestamp'].isna(), 'cantGetFirstTimestamp'] = False
    con_df.loc[con_df['hasTimestamp'].isna(), 'hasTimestamp'] = False
    # grouped = con_df.groupby(['expiryDate', 'hasTimestamp', 'cantGetFirstTimestamp'], as_index=False)
    # con_df = grouped.sum()

    dates = sorted(set(con_df.lastTradeDateOrContractMonth))
    date_lookup = {date: ind for ind, date in enumerate(dates)}
    con_df['ind'] = [date_lookup[my_key] for my_key in con_df.lastTradeDateOrContractMonth]


    return con_df


(selected_date_index, marks) = getMarks()

layout = html.Div([
    dcc.Link('Contract Timestamps By Date', href='/apps/contract_timestamps'),
    html.Label('Contract Timestamps By Symbol'),
    dcc.Link('Daily Bars By Date', href='/apps/daily_bars_by_date'),
    dcc.Link('Daily Bars', href='/apps/daily_bars'),
    dcc.RadioItems(
        id='right',
        options=[
            {'label': 'Calls', 'value': 'C'},
            {'label': 'Puts', 'value': 'P'},
        ],
        value='C'
    ),
    dcc.Dropdown(
        id='symbol',
        options=[
            {'label': 'SPY', 'value': 'SPY'},
            {'label': 'QQQ', 'value': 'QQQ'},
            {'label': 'ES', 'value': 'ES'}
        ],
        value='SPY'
    ),
    dcc.Graph(
        style={'height': 500},
        id='my-timestamp-by-symbol-graph',
    ),
    dcc.Interval(
        id='interval-component',
        interval=60 * 1000,  # in milliseconds
        n_intervals=0
    ),

])


@app.callback(Output('my-timestamp-by-symbol-graph', 'figure'), [Input('right', 'value'),
                                                                 Input('symbol', 'value'),
                                                                 Input('interval-component', 'n_intervals')])
def update_timestamp_figure(right, symbol, n_intervals):
    print('hello')
    con_df = get_data(right, symbol)
    return get_points(con_df)
