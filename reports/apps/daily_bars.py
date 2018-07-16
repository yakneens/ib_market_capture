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
from dateutil.relativedelta import relativedelta,FR

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
    date_df = pd.read_sql(query, connection, parse_dates=["lastTradeDateOrContractMonth"])
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


(selected_date_index, marks) = getMarks()


layout = html.Div([
    dcc.Link('Contract Timestamps By Date', href='/apps/contract_timestamps'),
    dcc.Link('Contract Timestamps By Symbol', href='/apps/contract_timestamps_by_symbol'),
    dcc.Link('Daily Bars By Date', href='/apps/daily_bars_by_date'),
    html.Label('Daily Bars'),

    dcc.Graph(
        style={'height': 300, },
        id='my-timestamp-graph'
    ),
    dcc.Graph(
        style={'height': 300},
        id='my-daily-bar-graph'
    ),
    dcc.Slider(
        id='year-slider',
        value=selected_date_index,
        step=None,
        min=min(marks),
        max=max(marks),
        marks=marks,
        included=False,
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


def get_timestamp_bars(my_data, selected_date):
    trace = [
        go.Bar(
            x=my_data.query('hasTimestamp == True')[
                'symbol'],
            y=my_data.query('hasTimestamp == True')['count'],
            name='Has Timestamp',
            marker=go.Marker(color=has_color),
            #, line=dict(color='rgb(0, 0, 0)',width=1)
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


@app.callback(Output('my-daily-bar-graph', 'figure'), [Input('year-slider', 'value'),
                                                       Input('interval-component', 'n_intervals')])
def update_daily_bar_figure(selected_date_idx, n_intervals):
    my_dates = get_dates()
    selected_date = my_dates[selected_date_idx]
    con_df = get_daily_bar_data(selected_date)
    return get_daily_bar_bars(con_df, selected_date)


@app.callback(Output('my-timestamp-graph', 'figure'), [Input('year-slider', 'value'),
                                                       Input('interval-component', 'n_intervals')])
def update_timestamp_figure(selected_date_idx, n_intervals):
    my_dates = get_dates()
    selected_date = my_dates[selected_date_idx]
    con_df = get_timestamp_data(selected_date)
    return get_timestamp_bars(con_df, selected_date)
