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


def getMarks(daterange):
    my_range = pd.date_range(daterange[0], daterange[1], freq='M')
    result = {}
    for i, date in enumerate(my_range):
        result[unixTimeMillis(date)] = {"label": str(date.strftime('%d-%b-%y')),
                                        "style": {"transform": "rotate(90deg)",
                                                  "white-space": "nowrap",
                                                  "margin-top": "2%"}}

    return result


def get_data():
    query = 'select count(*),c."lastTradeDateOrContractMonth"::date as "expiryDate",c."cantGetFirstTimestamp",t."firstTimestamp" is not null AS "hasTimestamp" ' \
            'from contracts c left join contract_ib_first_timestamp t on c."conId" = t."contractId" ' \
            'group by  "expiryDate", c."cantGetFirstTimestamp", "hasTimestamp" ' \
            'order by  "expiryDate", "hasTimestamp"'

    con_df = pd.read_sql(query, connection, parse_dates=["expiryDate"])
    # con_df['expiryDate'] = con_df['lastTradeDateOrContractMonth'].apply(
    #     lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
    con_df.loc[con_df['cantGetFirstTimestamp'].isna(), 'cantGetFirstTimestamp'] = False
    con_df.loc[con_df['hasTimestamp'].isna(), 'hasTimestamp'] = False
    # grouped = con_df.groupby(['expiryDate', 'hasTimestamp', 'cantGetFirstTimestamp'], as_index=False)
    # con_df = grouped.sum()

    return con_df


# Set upper limit to 2021 because otherwise the scale has too many values
con_df = get_data().query('expiryDate < 2021')
val_range = getMarks([con_df['expiryDate'].min(), con_df['expiryDate'].max()])
interval_counter = 0

layout = html.Div(className='container',
                  children=[
                      html.Nav(className='navbar navbar-expand-lg navbar-light bg-light nav-tabs nav-fill', children=[
                          html.A('Timestamps By Date', href='/apps/contract_timestamps',
                                 className='nav-item nav-link btn btn-outline-success active'),
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
                      ]),
                      dcc.Checklist(
                          id='expiry-day-of-week',
                          options=[
                              {'label': 'Monday', 'value': 0},
                              {'label': 'Tuesday', 'value': 1},
                              {'label': 'Wednesday', 'value': 2},
                              {'label': 'Thursday', 'value': 3},
                              {'label': 'Friday', 'value': 4}
                          ],
                          values=[0, 1, 2, 3, 4],
                          labelClassName='checkbox-inline',
                          inputClassName='checkbox'
                      ),
                      dcc.Graph(
                          style={'height': 300},
                          id='my-graph'
                      ),
                      dcc.RangeSlider(
                          id='year-slider',
                          min=min(val_range.keys()),
                          max=max(val_range.keys()),
                          value=[unixTimeMillis(datetime.datetime.now() - timedelta(days=7)),
                                 unixTimeMillis(datetime.datetime.now() + timedelta(weeks=8))],
                          step=None,
                          marks=val_range,
                      ),
                      dcc.Interval(
                          id='interval-component',
                          interval=60 * 1000,  # in milliseconds
                          n_intervals=0
                      ),

                  ])


def get_bars(my_data):
    trace = [
        go.Bar(
            x=my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True')[
                'expiryDate'],
            y=my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True')['count'],
            name='No Timestamp',
            marker=go.Marker(color=hasnt_color),
        ),

        go.Bar(
            x=my_data.query('hasTimestamp == True')[
                'expiryDate'],
            y=my_data.query('hasTimestamp == True')['count'],
            name='Has Timestamp',
            marker=go.Marker(color=has_color),
        ),
        go.Bar(
            x=my_data.query('cantGetFirstTimestamp == True')['expiryDate'],
            y=my_data.query('cantGetFirstTimestamp == True')['count'],
            name='Can\'t Get Timestamp',
            marker=go.Marker(color=cant_color),
        ),
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
            margin=go.Margin(l=40, r=40, t=40, b=30),
            barmode='group'
        )
    }


@app.callback(Output('my-graph', 'figure'), [Input('year-slider', 'value'),
                                             Input('expiry-day-of-week', 'values'),
                                             Input('interval-component', 'n_intervals')])
def update_figure(selected_dates, selected_expiry_days, n_intervals):
    con_df = get_data()

    filtered_df = con_df[
        (con_df.expiryDate >= unixToDatetime(selected_dates[0])) &
        (con_df.expiryDate <= unixToDatetime(selected_dates[1]))
        ]

    filtered_df = filtered_df[[x in selected_expiry_days for x in
                               np.vectorize(lambda x: pd.to_datetime(x).weekday())(filtered_df.expiryDate.values)]]
    return get_bars(filtered_df)
