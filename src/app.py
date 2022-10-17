
import requests
import flask
import traceback

import requests
import json
import pandas as pd

# Dash Framework
import dash_bootstrap_components as dbc
from dash import Dash, callback, clientside_callback, html, dcc, dash_table as dt, Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate


server = flask.Flask('app')

# ---------------------------------
#   Get Data From datastore
# ---------------------------------

def get_api_data(api_address):
    api_json = {}
    try:
        try:
            response = requests.get(api_address)
        except:
            return('error: {}'.format(e))
        request_status = response.status_code
        if request_status == 200:
            api_json = response.json()
            return api_json
        else:
            return request_status
    except Exception as e:
        traceback.print_exc()
        api_json['json'] = 'error: {}'.format(e)
        return api_json

#
# print("data from datastore:", datafeed)

# ---------------------------------
#   Page components
# ---------------------------------
def serve_layout():

    layout = html.Div([
        dcc.Store(id='store_data'),
        html.H1('A2CPS Data from API'),
        dbc.Row([
            dbc.Col([
                html.P('Call for new data from APIs (if needed):'),
                dcc.Dropdown(
                    id='dropdown-api',
                   options=[
                        # {'label': 'APIs', 'value': 'apis'},
                        {'label': 'Consort', 'value': 'consort'},
                       {'label': 'Subjects', 'value': 'subjects'},
                       {'label': 'Imaging', 'value': 'imaging'},
                       {'label': 'Blood Draws', 'value': 'blood'},
                   ],
                   # value='apis'
                ),
                html.Button('Reload API', id='submit-api', n_clicks=0),
                html.P('Available Data / DataFrames '),
                dcc.Loading(
                    id="loading-content",
                    type="default",
                    children = [
                        dcc.Dropdown(
                            id ='dropdown_datastores'
                        ),
                    ]
                    # children=[
                    #         html.Div(id='div_content'),
                    #         html.Div(id='div_table')
                    #     ]
                ),

                html.Div(id='df-columns'),
            ],width=2),
            dbc.Col([
                dcc.Store(id='store-table'),
                html.Div(id='div-content')
            ], width=10),
        ]),


    ])
    return layout

# ---------------------------------
#   build app
# ---------------------------------
external_stylesheets_list = [dbc.themes.SANDSTONE, 'https://codepen.io/chriddyp/pen/bWLwgP.css'] #  set any external stylesheets

app = Dash('app', server=server,
                external_stylesheets=external_stylesheets_list,
                suppress_callback_exceptions=True,
                meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}])

app.layout = serve_layout

if __name__ == '__main__':
    app.run_server()


# ---------------------------------
#   Callbacks
# ---------------------------------
@app.callback(
    Output('store_data', 'data'),
    Output('dropdown_datastores', 'options'),
    Input('submit-api', 'n_clicks'),
    State('dropdown-api', 'value'),
    State('store_data', 'data')
)
def update_datastore(n_clicks, api, datastore_dict):
    if n_clicks == 0:
        raise PreventUpdate
    if not datastore_dict:
        datastore_dict = {}
    api_json = {}
    print(api)
    if api:
        api_address = "http://datastore:8050/api/" + api
        api_json = get_api_data(api_address)
        if api_json:
            datastore_dict[api] = api_json
            print('got api-json')
        else:
            print('no api-json')

    options = []
    if datastore_dict:
        for api in datastore_dict.keys():
            api_label = api + ' [' + datastore_dict[api]['date'] + ']'
            api_header_option = {'label': api_label, 'value': api_label, 'disabled': True}
            options.append(api_header_option)
            for dataframe in datastore_dict[api]['data'].keys():
            #     api_dataframe_label = api + '_' + datastore_dict[api][dataframe]
                # api_dataframe_option = {'label': api_dataframe_label, 'value': api_dataframe_label}
                api_dataframe_option = {'label': '  -' + dataframe, 'value': api + ':' + dataframe}
                options.append(api_dataframe_option)
        # print(datastore_dict[api]['date'])
        # print(datastore_dict[api]['data'].keys())

    # options = list(datastore_dict[api]['data'].keys())
    # print(datastore_dict.keys())
    # for key in datastore_dict.keys():
    #     if datastore_dict[key] is dict:
    #         print(datastore_dict[key].keys())
    #         for k in datastore_dict[key].keys():
    #             if datastore_dict[k] is dict:
    #                 print(datastore_dict[key][k].keys())
    else:
        print('no datastore_dict')
    return datastore_dict, options

@app.callback(
    # Output('dropdown_datastores', 'options'), store-table
    Output('div-content','children'),
    Input('dropdown_datastores', 'value'),
    State('store_data', 'data')
)
def show_table(selected_dataframe, datastore_dict):
    if selected_dataframe:
        if selected_dataframe == 'consort':
            return html.P('Consort')
        else:
            api, dataframe = selected_dataframe.split(':')
            print(api, dataframe )
            print(datastore_dict[api]['data'][dataframe][0])
            div_table = dt.DataTable(
                data=datastore_dict[api]['data'][dataframe],
                virtualization=True,
                    style_table={
                    'overflowX': 'auto',
                    'width':'100%',
                    'margin':'auto'},
                    page_current= 0,
                    page_size= 15,
                # columns=[{"name": i, "id": i} for i in df.columns]
            )
            columns_list = list(datastore_dict[api]['data'][dataframe][0].keys())
            # return html.P('stuff here')
            columns_div = html.Div([
                html.P('Table Columns:'), html.P(', '.join(columns_list))
                ])
            return html.Div([columns_div, div_table])
    else:
        return html.P('')

# @app.callback(
#     Output('div_content', 'children'),
#     Ouput('store_data', 'data'),
#     Input('dropdown-api', 'value'),
#     State('store_data', 'data')
# )
# def update_content(api, data):
#     div_json = ''
#     if api:
#         api_address = "http://datastore:8050/api/" + api
#         div_json = get_api_data(api_address)
#     # div_dict = json.loads(div_json)
#     # kids = html.Div([html.P(k) for key in div_dict.keys()])
#     k = list(div_json.keys())
#     k_date =k[0]
#     df_keys = list(div_json[k_date].keys())
#     kids = []
#     for key in df_keys:
#         kids.append(html.P(key))
#     dropdwn_dfs = dcc.Dropdown(
#            options=df_keys
#         )
#     return html.Div([html.P('Please select a dataframe:'), dropdwn_dfs ])
