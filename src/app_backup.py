
import flask
import traceback
import os
import sqlite3

import requests
import json
import pandas as pd

# Dash Framework
import dash_bootstrap_components as dbc
from dash import Dash, callback, clientside_callback, html, dcc, dash_table as dt, Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate


server = flask.Flask('app')

# ---------------------------------
#   MOVE THIS TO REFERENCE FROM ENV
# ---------------------------------
DATASTORE_URL = "http://datastore:8050/api/"

current_folder = os.path.dirname(__file__)
ASSETS_PATH = os.path.join(current_folder,'assets')
screening_sites = pd.read_csv(os.path.join(ASSETS_PATH,'screening_sites.csv'))

adverse_events = {}
consented = {}
subjects = {}

# ---------------------------------
#   Get Data From datastore
# ---------------------------------

def get_api_data(api_address):
    api_json = {}
    try:
        try:
            response = requests.get(api_address)
        except Exception as e:
            return('error: {}'.format(e))
        request_status = response.status_code
        if request_status == 200:
            api_json = response.json()
            return api_json
        else:
            return {'request: ' : str(request_status)}
    except Exception as e:
        traceback.print_exc()
        api_json['json'] = 'error: {}'.format(e)
        return api_json

#
# print("data from datastore:", datafeed)

def add_screening_site(screening_sites, df, id_col):
    # Get dataframes
    ids = df.loc[:, [id_col]]

    # open sql connection to create new datarframe with record_id paired to screening site
    conn = sqlite3.connect(':memory:')
    ids.to_sql('ids', conn, index=False)
    screening_sites.to_sql('ss', conn, index=False)

    sql_qry = f'''
    select {id_col}, screening_site, site, surgery_type, record_id_start, record_id_end
    from ids
    join ss on
    ids.{id_col} between ss.record_id_start and ss.record_id_end
    '''
    sites = pd.read_sql_query(sql_qry, conn)
    conn.close()

    df = sites.merge(df, how='left', on=id_col)

    return df #add_screening_site(screening_sites, subjects, 'record_id')

# ---------------------------------
#   Page components
# ---------------------------------

def serve_layout():
    api = 'subjects'
    # api = 'tester'

    global DATASTORE_URL
    global adverse_events
    global consented
    global subjects

    api_address = DATASTORE_URL + api
    api_json = get_api_data(api_address)

    subjects = pd.DataFrame.from_dict(api_json['data']['subjects_cleaned'])
    adverse_events = pd.DataFrame.from_dict(api_json['data']['adverse_events'])
    consented = pd.DataFrame.from_dict(api_json['data']['consented'])

    s2 = add_screening_site(screening_sites, subjects, 'record_id').head(10)

    layout = html.Div([
        dcc.Store(id='api_data', data = api_json),
        html.H1('Data from ' + api + ' api'),
        dbc.Row([
            dbc.Col([
                html.Div(
                    [html.P(c) for c in subjects.columns]
                ),
            ],width=2),
            dbc.Col([
                html.H2('Weekly Report'),
                dcc.Loading(
                    id="loading-content",
                    type="default",

                    children = [
                    # html.P('Data from: ' + api_json['date']),
                    # html.P('Available Dataframes: ') ,
                    # html.Div([
                    #     html.P(k) for k in api_json['data']['subjects'][0].keys()
                    # ]),
                    # html.Div(json.dumps(api_json))
                    html.P('Subjects'),
                    dt.DataTable(
                        subjects.to_dict('records'),
                        [{"name": i, "id": i} for i in subjects.columns],
                        id='tbl-subjects',
                        editable=True,
                        filter_action="native",
                        sort_action="native",
                        sort_mode="multi",
                        column_selectable="single",
                        row_selectable="multi",
                        row_deletable=True,
                        selected_columns=[],
                        selected_rows=[],
                        page_action="native",
                        page_current= 0,
                        page_size= 10,
                        style_table={'overflowX': 'scroll'},
                    ),
                    html.P('Sites'),
                    dt.DataTable(
                        s2.to_dict('records'),
                        [{"name": i, "id": i} for i in s2.columns],
                        # id='tbl-screening_sites',
                        # editable=True,
                        # filter_action="native",
                        # sort_action="native",
                        # sort_mode="multi",
                        # column_selectable="single",
                        # row_selectable="multi",
                        # row_deletable=True,
                        # selected_columns=[],
                        # selected_rows=[],
                        # page_action="native",
                        # page_current= 0,
                        # page_size= 10,
                        style_table={'overflowX': 'scroll'},
                    ),

                    # html.P('Consented'),
                    # dt.DataTable(
                    #     consented.to_dict('records'),
                    #     [{"name": i, "id": i} for i in consented.columns],
                    #     id='tbl-consented',
                    #     editable=True,
                    #     filter_action="native",
                    #     sort_action="native",
                    #     sort_mode="multi",
                    #     column_selectable="single",
                    #     row_selectable="multi",
                    #     row_deletable=True,
                    #     selected_columns=[],
                    #     selected_rows=[],
                    #     page_action="native",
                    #     page_current= 0,
                    #     page_size= 10,
                    #     style_table={'overflowX': 'scroll'},
                    # ),
                    # html.P('adverse_events'),
                    # dt.DataTable(
                    #     adverse_events.to_dict('records'),
                    #     [{"name": i, "id": i} for i in adverse_events.columns],
                    #     id='tbl-adverse',
                    #     editable=True,
                    #     filter_action="native",
                    #     sort_action="native",
                    #     sort_mode="multi",
                    #     column_selectable="single",
                    #     row_selectable="multi",
                    #     row_deletable=True,
                    #     selected_columns=[],
                    #     selected_rows=[],
                    #     page_action="native",
                    #     page_current= 0,
                    #     page_size= 10,
                    #     style_table={'overflowX': 'scroll'},
                    # ),
                ]
                ),

            ], width=10)
        ]),


    ])
    return layout

def test_layout():
    layout = html.Div([
        html.P('test Layout')
    ])
    return layout

def test_layout_subjects():
    api='subjects'
    api_address = "http://datastore:8050/api/" + api
    api_json = get_api_data(api_address)

    subjects = pd.DataFrame.from_dict(api_json['data']['subjects_cleaned'])
    adverse_events = pd.DataFrame.from_dict(api_json['data']['adverse_events'])
    consented = pd.DataFrame.from_dict(api_json['data']['consented'])

    # # print('GET TABLE DATA')
    # table1, table2a, table2b, table3, table4, table5, table6, table7a, table7b, table8a, table8b, sex, race, ethnicity, age = get_tables(today, start_report, end_report, report_date_msg, report_range_msg, display_terms, display_terms_dict, display_terms_dict_multi, subjects, consented, adverse_events, centers_df)


    layout = html.Div([
        # html.H1('Load'),
        html.P('Subjects'),
        html.P([api_json['date']]),

        html.Div([html.P(key) for key in api_json['data'].keys()]),
        # html.Div(json.dumps(api_json)),
        dt.DataTable(
            subjects.to_dict('records'),
            [{"name": i, "id": i} for i in subjects.columns],
            id='tbl-subjects',
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            page_action="native",
            page_current= 0,
            page_size= 10,
            style_table={'overflowX': 'scroll'},
        ),
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

# app.layout = serve_layout
app.layout = test_layout

if __name__ == '__main__':
    app.run_server()


# ---------------------------------
#   Callbacks
# ---------------------------------
