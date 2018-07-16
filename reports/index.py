
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from app import app
from apps import contract_timestamps, daily_bars, daily_bars_by_date, contract_timestamps_by_symbol


app.layout = html.Div([
    html.Link(href='/static/style.css', rel='stylesheet'),
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/contract_timestamps':
         return contract_timestamps.layout
    elif pathname == '/apps/contract_timestamps_by_symbol':
         return contract_timestamps_by_symbol.layout
    elif pathname == '/apps/daily_bars':
         return daily_bars.layout
    elif pathname == '/apps/daily_bars_by_date':
         return daily_bars_by_date.layout
    else:
        return '404'

if __name__ == '__main__':
    app.run_server(debug=True)