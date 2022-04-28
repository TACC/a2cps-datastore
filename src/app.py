from flask import Flask, jsonify
from os import environ
import os
import pandas as pd
import json

from data_loading import *

# ----------------------------------------------------------------------------
# DATA PARAMETERS
# ----------------------------------------------------------------------------
current_folder = os.path.dirname(__file__)
DATA_PATH = os.path.join(current_folder,'data')
ASSETS_PATH = os.path.join(current_folder,'assets')


# Path to Report files at TACC
api_root = 'https://api.a2cps.org/files/v2/download/public/system/a2cps.storage.community/reports'


# ----------------------------------------------------------------------------
# LOAD ASSETS FILES
# ----------------------------------------------------------------------------
asset_files_dict = {
    'screening_sites': 'screening_sites.csv',
    'display_terms': 'A2CPS_display_terms.csv',
}

print("loading display terms")
display_terms, display_terms_dict, display_terms_dict_multi = load_display_terms(ASSETS_PATH, asset_files_dict['display_terms'])

print("loading screening sites")
screening_sites = pd.read_csv(os.path.join(ASSETS_PATH,asset_files_dict['screening_sites']))

# ----------------------------------------------------------------------------
# LOAD INITAL DATA FROM FILES
# ----------------------------------------------------------------------------

print("get local imaging data")
local_date = '2022-04-13'
local_imaging_data = {
    'date': local_date,
    'data': get_local_imaging_data(DATA_PATH)}

print("get local blood data")
local_blood_data = {
    'date': local_date,
    'data': get_local_blood_data(DATA_PATH)}


print("get local subjects")
local_subjects_json = get_local_subjects_raw(DATA_PATH)
local_subjects_data = {
    'date': local_date,
    'data': create_clean_subjects(local_subjects_json, screening_sites, display_terms_dict, display_terms_dict_multi)}


print("loading local data complete")
# ----------------------------------------------------------------------------
# APIS
# ----------------------------------------------------------------------------
api_imaging_data_cache = []
api_blood_data_cache = []
api_subjects_json_cache = []

app = Flask(__name__)

# APIS: try to load new data, if doesn't work, get most recent
@app.route("/api/imaging")
def api_imaging():
    print("GET /api/imaging")
    global api_imaging_data_cache
    try:
        if not check_available_data(api_imaging_data_cache):
            current_date = str(datetime.now())
            latest_data = get_api_imaging_data()

            api_imaging_data = {
                'date': current_date,
                'data': latest_data
                }
            api_imaging_data_cache = [api_imaging_data]
        else:
            api_imaging_data_cache = api_imaging_data_cache
        return jsonify(api_imaging_data_cache[-1])

    except Exception as e:
        traceback.print_exc()
        try:
            return jsonify(local_imaging_data)
        except:
            return jsonify('error: {}'.format(e))

@app.route("/api/blood")
def api_blood():
    print("GET /api/blood")
    global api_blood_data_cache
    try:
        if not check_available_data(api_blood_data_cache):
            current_date = str(datetime.now())
            latest_data = get_api_blood_data()

            api_blood_data = {
                'date': current_date,
                'data': latest_data
                }
            api_blood_data_cache = [api_blood_data]
        else:
            api_blood_data_cache = api_blood_data_cache
        return jsonify(api_blood_data_cache[-1])

    except Exception as e:
        traceback.print_exc()
        try:
            return jsonify(local_blood_data)
        except:
            return jsonify('error: {}'.format(e))


@app.route("/api/subjects")
def api_subjects():
    print("GET /api/subjects")
    global api_subjects_json_cache
    try:
        if not check_available_data(api_subjects_json_cache):
            current_date = str(datetime.now())
            latest_subjects_json = get_api_subjects_json()
            if latest_subjects_json:
                latest_data = create_clean_subjects(latest_subjects_json, screening_sites, display_terms_dict, display_terms_dict_multi)
                api_subjects_data = {
                    'date': current_date,
                    'data': latest_data
                    }

                api_subjects_json_cache = [api_subjects_json]

            else:
                api_subjects_json_cache = api_subjects_json_cache
            return jsonify(api_subjects_json_cache[-1])

    except Exception as e:
        traceback.print_exc()
        try:
            return jsonify(local_subjects_data)
        except:
            return jsonify('error: {}'.format(e))


@app.route("/api/full")
def api_full():
    print("GET /api/full")
    datafeeds = {'date': {'weekly': 'today', 'consort': 'today', 'blood': 'today'},
                'data': {'weekly': 'tbd', 'consort': 'tbd', 'blood': 'tbd'}}
    return jsonify(datafeeds)


@app.route("/api")
def api():
    print("GET /api")
    return jsonify({})


if __name__ == "__main__":
    app.run(host='0.0.0.0')
