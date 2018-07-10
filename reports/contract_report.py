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


engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]
contracts = meta.tables["contracts"]

query = 'select count(*),c."lastTradeDateOrContractMonth",c."cantGetFirstTimestamp",t."firstTimestamp" is not null AS "hasTimestamp" ' \
        'from contracts c left join contract_ib_first_timestamp t on c."conId" = t."contractId" ' \
        'group by c."lastTradeDateOrContractMonth", c."cantGetFirstTimestamp", t."firstTimestamp" is not null ' \
        'order by c."lastTradeDateOrContractMonth", t."firstTimestamp" is not null'
con_df = pd.read_sql(query, connection, parse_dates=["lastTradeDateOrContractMonth"])
con_df['expiryDate'] = con_df['lastTradeDateOrContractMonth'].apply(
    lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
con_df.loc[con_df['cantGetFirstTimestamp'].isna(), 'cantGetFirstTimestamp'] = False
con_df.loc[con_df['hasTimestamp'].isna(), 'hasTimestamp'] = False
grouped = con_df.groupby(['expiryDate', 'hasTimestamp', 'cantGetFirstTimestamp'], as_index=False)
con_df = grouped.sum()

# con_df = con_df.query('expiryDate < 2019')

# con_df['lastTradeDateOrContractMonth'] = datetime.datetime.strptime(con_df['lastTradeDateOrContractMonth'], "%Y%m%d")

layout = go.Layout(
    barmode='stack',
    title='Stacked Bar with Pandas'
)

app = dash.Dash()
app.css.config.serve_locally = True
app.scripts.config.serve_locally = True

val_range = getMarks([con_df['expiryDate'].min(), con_df['expiryDate'].max()])

app.layout = html.Div([
    dcc.Checklist(
        id='expiry-day-of-week',
        options=[
            {'label': 'Monday', 'value': 0},
            {'label': 'Wednesday', 'value': 2},
            {'label': 'Friday', 'value': 4}
        ],
        values=[0, 2, 4]
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

])


def get_bars(my_data):
    trace = [
        go.Bar(
            x=my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True')[
                'expiryDate'],
            y=my_data.query('cantGetFirstTimestamp != True & hasTimestamp != True')['count'],
            name='No Timestamp',
            marker=go.Marker(color='rgb(255, 100, 100)'),
        ),

        go.Bar(
            x=my_data.query('hasTimestamp == True')[
                'expiryDate'],
            y=my_data.query('hasTimestamp == True')['count'],
            name='Has Timestamp',
            marker=go.Marker(color='rgb(100, 255, 100)'),
        ),
        go.Bar(
            x=my_data.query('cantGetFirstTimestamp == True')['expiryDate'],
            y=my_data.query('cantGetFirstTimestamp == True')['count'],
            name='Can\'t Get Timestamp',
            marker=go.Marker(color='rgb(100, 100, 255)'),
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
                                             Input('expiry-day-of-week', 'values')])
def update_figure(selected_dates, selected_expiry_days):
    filtered_df = con_df[
        (con_df.expiryDate >= unixToDatetime(selected_dates[0])) &
        (con_df.expiryDate <= unixToDatetime(selected_dates[1]))
        ]

    filtered_df = filtered_df[[x in selected_expiry_days for x in np.vectorize(lambda x: pd.to_datetime(x).weekday())(filtered_df.expiryDate.values)]]
    return get_bars(filtered_df)


if __name__ == '__main__':
    app.run_server()
