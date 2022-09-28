from flask import Flask, jsonify
from os import environ
import os
import pandas as pd
import json
import csv

# from data_processing import *
from data_loading import *



## Get Parameters
SECRET_KEY = environ.get("SECRET_KEY")
print("SECRET KEY", SECRET_KEY)

# ----------------------------------------------------------------------------
# DATA PARAMETERS
# ----------------------------------------------------------------------------
current_folder = os.path.dirname(__file__)
DATA_PATH = os.path.join(current_folder,'data')
ASSETS_PATH = os.path.join(current_folder,'assets')


# Path to Report files at TACC
api_root = 'https://api.a2cps.org/files/v2/download/public/system/a2cps.storage.community/reports'

# Load subjects locally
# Opening JSON file
filepath = os.path.join(DATA_PATH,'lsj.json')
f = open(filepath)
# returns JSON object as a dictionary
subjects_raw = json.load(f)
# Closing file
f.close()

# ----------------------------------------------------------------------------
# LOAD ASSETS FILES
# ----------------------------------------------------------------------------
asset_files_dict = {
    'screening_sites': 'screening_sites.csv',
    'display_terms': 'A2CPS_display_terms.csv',
}

display_terms, display_terms_dict, display_terms_dict_multi = load_display_terms(ASSETS_PATH, asset_files_dict['display_terms'])

screening_sites = pd.read_csv(os.path.join(ASSETS_PATH,asset_files_dict['screening_sites']))

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


# local_subjects_json = get_local_subjects_raw(DATA_PATH)
local_subjects_data = {
    'date': local_date,
    'data': process_subjects(subjects_raw,screening_sites, display_terms_dict, display_terms_dict_multi)
    }

local_data = {
        'imaging': local_imaging_data,
        'blood': local_imaging_data,
        'subjects': local_subjects_data
}

# ----------------------------------------------------------------------------
# APIS
# ----------------------------------------------------------------------------
datetime_format = "%m/%d/%Y, %H:%M:%S"

apis_imaging_index = {}
data_state = 'empty'
api_data_index = {
    'blood':'',
    'imaging':'',
    'subjects':'',
    'raw': 'local'
}
api_request_state = {
    'blood':None,
    'imaging':None,
    'subjects1':None,
    'subjects2':None,
}
api_data_cache = {
    'blood':None,
    'imaging':None,
    'subjects':None,
    'raw': None
}

api_subjects = {'date':None, 'data':None}

app = Flask(__name__)

# APIS: try to load new data, if doesn't work, get most recent
@app.route("/api/apis")
def api_apis():
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
    global datetime_format
    global api_data_index
    global api_data_cache

    try:
        if not api_data_index['subjects'] or not check_data_current(datetime.strptime(api_data_index['subjects'], datetime_format)):
            api_date = datetime.now().strftime(datetime_format)
            latest_subjects_json = get_api_subjects_json()
            if latest_subjects_json:
                # latest_data = create_clean_subjects(latest_subjects_json, screening_sites, display_terms_dict, display_terms_dict_multi)
                latest_data = process_subjects(latest_subjects_json,screening_sites, display_terms_dict, display_terms_dict_multi)
                
                api_data_cache['subjects'] = latest_data
                api_data_index['subjects'] = api_date

        return jsonify({'date': api_data_index['subjects'], 'data': api_data_cache['subjects']})
    except Exception as e:
        traceback.print_exc()
        return jsonify('error: {}'.format(e))

@app.route("/api/load_data")
def api_load_data():
    global datetime_format
    global api_data_index
    global api_data_cache

    try:
        # return api_data_index
        # if not 'tester' in api_data_index.keys():
        if not api_request_state['subjects1']  == 200 or not api_request_state['subjects2'] == 200 :
            api_date = datetime.now().strftime(datetime_format)
            api_data_index['raw'] = api_date

            # latest_subjects_json = get_api_subjects_json()

            api_root = 'https://api.a2cps.org/files/v2/download/public/system/a2cps.storage.community/reports'

            # Load Json Data
            subjects1_filepath = '/'.join([api_root,'subjects','subjects-1-latest.json'])
            subjects1_request = requests.get(subjects1_filepath)
            api_request_state['subjects1'] = subjects1_request.status_code
            if subjects1_request.status_code == 200:
                subjects1 = subjects1_request.json()
            else:
                subjects1 = subjects1_request.status_code
                # return {'status':'500', 'source': api_dict['subjects']['subjects1']}

            subjects2_filepath = '/'.join([api_root,'subjects','subjects-2-latest.json'])
            subjects2_request = requests.get(subjects2_filepath)
            api_request_state['subjects2'] = subjects2_request.status_code
            if subjects2_request.status_code == 200:
                subjects2 = subjects2_request.json()
            else:
                subjects2 = subjects2_request.status_code
                # return {'status':'500', 'source': api_dict['subjects']['subjects2']}

            # Create combined json
            latest_subjects_json = {'1': subjects1, '2': subjects2}
            api_data_cache['raw'] = latest_subjects_json

            if latest_subjects_json:
                api_data_cache['raw'] = latest_subjects_json
                return jsonify(api_data_cache['raw'])
            else:
                api_data_cache['raw'] = 'failed'
                return jsonify({'failed':''})


    except Exception as e:
        traceback.print_exc()
        return jsonify('error: {}'.format(e))


@app.route("/api/tester")
def api_tester():

    global local_subjects_data

    try:
        return jsonify(local_subjects_data)


    except Exception as e:
        traceback.print_exc()
        return jsonify('error: {}'.format(e))

# @app.route("/api/screening_sites")
# def api_screening_sites():
#     screening_site_dict = screening_sites.to_dict('records')
#     return jsonify(screening_site_dict)

# @app.route("/api/subjects")
# def api_subjects():
#
#     global api_subjects_json_cache
#     try:
#         if not check_available_data(api_subjects_json_cache):
#             current_date = str(datetime.now())
#             latest_subjects_json = get_api_subjects_json()
#             if latest_subjects_json:
#                 latest_data = create_clean_subjects(latest_subjects_json, screening_sites, display_terms_dict, display_terms_dict_multi)
#                 api_subjects_data = {
#                     'date': current_date,
#                     'data': latest_data
#                     }
#
#                 api_subjects_json_cache = [api_subjects_json]
#
#             else:
#                 api_subjects_json_cache = api_subjects_json_cache
#             return jsonify(api_subjects_json_cache[-1])
#
#     except Exception as e:
#         traceback.print_exc()
#         # try:
#         #     return jsonify(local_data['subjects'])
#         # except:
#         return jsonify('error: {}'.format(e))


@app.route("/api/full")
def api_full():
    datafeeds = {'date': {'weekly': 'today', 'consort': 'today', 'blood': 'today'},
                'data': {'weekly': 'tbd', 'consort': 'tbd', 'blood': 'tbd'}}
    return jsonify(datafeeds)




if __name__ == "__main__":
    app.run(host='0.0.0.0')
