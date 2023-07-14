from flask import Flask, jsonify, request
from os import environ
import os
import pandas as pd
import json
import csv

# from data_processing import *
from data_loading import *

## Demonstrate that app is accessing the env variables properly
SECRET_KEY = environ.get("SECRET_KEY")
#print("SECRET KEY", SECRET_KEY)

# ----------------------------------------------------------------------------
# DATA PARAMETERS
# ----------------------------------------------------------------------------
current_folder = os.path.dirname(__file__)
DATA_PATH = os.path.join(current_folder,'data')
ASSETS_PATH = os.path.join(current_folder,'assets')


# Path to Report files at TACC
api_root = environ.get("API_ROOT") #'https://api.a2cps.org/files/v2/download/public/system/a2cps.storage.community/reports'

# ----------------------------------------------------------------------------
# LOAD ASSETS FILES
# ----------------------------------------------------------------------------
asset_files_dict = {
    'screening_sites': 'screening_sites.csv',
    'display_terms': 'A2CPS_display_terms.csv',
}

display_terms, display_terms_dict, display_terms_dict_multi = load_display_terms(ASSETS_PATH, asset_files_dict['display_terms'])

screening_sites = pd.read_csv(os.path.join(ASSETS_PATH,asset_files_dict['screening_sites']))


# Columns used in reports [UPDATE THIS IF START TO USE MORE]
subjects_raw_cols_for_reports = ['ewcomments',
 'start_v3_3mo',
 'start_12mo',
 'sp_inclage1884',
 'start_v2_6wk',
 'obtain_date',
 'sp_inclcomply',
 'participation_interest',
 'sp_inclsurg',
 'sp_exclnoreadspkenglish',
 'ptinterest_comment',
 'reason_not_interested',
 'start_v1_preop',
 'sp_exclarthkneerep',
 'sp_surg_date',
 'sp_exclprevbilthorpro',
 'sp_exclothmajorsurg',
 'sp_exclbilkneerep',
 'age',
 'sp_exclinfdxjoint',
 'screening_age',
 'start_6mo',
 'main_record_id',
 'sp_mricompatscr',
 'ewdateterm']
# ----------------------------------------------------------------------------
# LOAD INITAL DATA FROM FILES
# ----------------------------------------------------------------------------

local_date = '2022-09-08'

local_imaging_data = {
    'date': local_date,
    'data': get_local_imaging_data(DATA_PATH)}

local_blood_data = {
    'date': local_date,
    'data': get_local_blood_data(DATA_PATH)}

subjects_raw = get_local_subjects_raw(DATA_PATH)
local_subjects_data = {
    'date': local_date,
    'data': process_subjects_data(subjects_raw,subjects_raw_cols_for_reports,screening_sites, display_terms_dict, display_terms_dict_multi)
    }

local_data = {
        'imaging': local_imaging_data,
        'blood': local_imaging_data,
        'subjects': local_subjects_data
}

# ----------------------------------------------------------------------------
# APIS
# ----------------------------------------------------------------------------
print('apis')
datetime_format = "%m/%d/%Y, %H:%M:%S"

apis_imaging_index = {}
data_state = 'empty'
api_data_index = {
    'blood':'',
    'imaging':'',
    'subjects':'',
    'consort':'',
}
api_request_state = {
    'blood':None,
    'imaging':None,
    'subjects1':None,
    'subjects2':None,
    'consort':None,
}
api_data_cache = {
    'blood':None,
    'imaging':None,
    'subjects':None,
    'raw': None,
    'consort':None,
}

# ----------------------------------------------------------------------------
# SIMPLE APIS
# ----------------------------------------------------------------------------
print('simple apis')
api_data_simple = {
    'blood':None,
    'imaging':None,
    'subjects':None,
    'raw': None
}

app = Flask(__name__)

# APIS: try to load new data, if doesn't work, get most recent
@app.route("/api/apis")
def api_apis():
    print('api_apis')
    print(api_data_index)
    return jsonify(api_data_index)

@app.route("/api/imaging")
def api_imaging():
    global datetime_format
    global api_data_index
    global api_data_cache
    try:
        if not api_data_index['imaging'] or not check_data_current(datetime.strptime(api_data_index['imaging'], datetime_format)):
            api_date = datetime.now().strftime(datetime_format)
            imaging_data = get_api_imaging_data()
            if imaging_data:
                api_data_cache['imaging'] = imaging_data
                api_data_index['imaging'] = api_date
        return jsonify({'date': api_data_index['imaging'], 'data': api_data_cache['imaging']})
    except Exception as e:
        traceback.print_exc()
        return jsonify('error: {}'.format(e))

@app.route("/api/consort")
def api_consort():
    global datetime_format
    global api_data_index
    global api_data_cache
    # try:
    if not api_data_index['consort'] or not check_data_current(datetime.strptime(api_data_index['consort'], datetime_format)):
        api_date = datetime.now().strftime(datetime_format)
        consort_data_json = get_api_consort_data()
        if consort_data_json:
            api_data_cache['consort'] = consort_data_json
            api_data_index['consort'] = api_date
    return jsonify({'date': api_data_index['consort'], 'data': api_data_cache['consort']})
    # except Exception as e:
    #     traceback.print_exc()
    #     return jsonify('error: {}'.format(e))

# get_api_consort_data
@app.route("/api/blood")
def api_blood():
    global datetime_format
    global api_data_index
    global api_data_cache
    try:
        if not api_data_index['blood'] or not check_data_current(datetime.strptime(api_data_index['blood'], datetime_format)):
            api_date = datetime.now().strftime(datetime_format)
            blood_data, blood_data_request_status = get_api_blood_data()
            if blood_data:
                api_data_index['blood'] = api_date
                api_data_cache['blood'] = blood_data

            with open('requests.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                for i in blood_data_request_status:
                    writer.writerow(i)
                f.close()

        return jsonify({'date': api_data_index['blood'], 'data': api_data_cache['blood']})
    except Exception as e:
        traceback.print_exc()
        return jsonify('error: {}'.format(e))


@app.route("/api/subjects")
def api_subjects():
    print('api_subjects')
    print(request.json)
    global datetime_format
    global api_data_index
    global api_data_cache
    global subjects_raw_cols_for_reports

    try:
        if not api_data_index['subjects'] or not check_data_current(datetime.strptime(api_data_index['subjects'], datetime_format)):
            api_date = datetime.now().strftime(datetime_format)
            latest_subjects_json = get_api_subjects_json(os.environ.get('API_ROOT'), request.args.get('coresessionid'))
            if latest_subjects_json:
                # latest_data = create_clean_subjects(latest_subjects_json, screening_sites, display_terms_dict, display_terms_dict_multi)
                latest_data = process_subjects_data(latest_subjects_json,subjects_raw_cols_for_reports,screening_sites, display_terms_dict, display_terms_dict_multi)

                api_data_cache['subjects'] = latest_data
                api_data_index['subjects'] = api_date

        return jsonify({'date': api_data_index['subjects'], 'data': api_data_cache['subjects']})
    except Exception as e:
        traceback.print_exc()
        return jsonify('error: {}'.format(e))
def api_tester():

    global local_subjects_data

    try:
        return jsonify(local_subjects_data)


    except Exception as e:
        traceback.print_exc()
        return jsonify('error: {}'.format(e))


@app.route("/api/full")
def api_full():
    print('api_full')
    datafeeds = {}
    for data_category in api_data_cache:
        if api_data_cache[data_category]['data']:
                datafeeds[data_category] = list(api_data_cache[data_category]['data'].keys())
        else:
            datafeeds[data_category] = ['no data']
    return jsonify(datafeeds)

@app.route("/api/simple")
def api_simple():
    if api_data_simple['subjects']:
        return jsonify('simple subjects')
    else:
        return jsonify('not found')




if __name__ == "__main__":
    app.run(host='0.0.0.0')
