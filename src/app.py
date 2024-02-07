from datetime import datetime
from flask import Flask, jsonify, request
from os import environ
import os
import pandas as pd
import csv
import logging

# from data_processing import *
from data_loading import *

# ----------------------------------------------------------------------------
# ENV Variables & DATA PARAMETERS
# ----------------------------------------------------------------------------
data_access_type = os.environ.get('DATA_ACCESS_TYPE')

current_folder = os.path.dirname(__file__)
DATA_PATH = os.path.join(current_folder,'data')
ASSETS_PATH = os.path.join(current_folder,'assets')
# Path to Report files at TACC
api_root = environ.get("API_ROOT") 

local_data_path = os.environ.get("LOCAL_DATA_PATH","")
local_data_date = os.environ.get("LOCAL_DATA_DATE","")

if data_access_type == "LOCAL":

    imaging_filepath = os.path.join(local_data_path,os.environ.get("IMAGING_FILE"))
    qc_filepath = os.path.join(local_data_path,os.environ.get("QC_FILE"))
    blood1_filepath = os.path.join(local_data_path,os.environ.get("BLOOD1_FILE"))
    blood2_filepath = os.path.join(local_data_path,os.environ.get("BLOOD2_FILE"))
    subjects1_filepath = os.path.join(local_data_path,os.environ.get("SUBJECTS1_FILE"))
    subjects2_filepath = os.path.join(local_data_path,os.environ.get("SUBJECTS2_FILE"))
    monitoring_data_filepath = os.path.join(local_data_path,os.environ.get("MONITORING_FILE"))

    print(local_data_path, local_data_date, subjects1_filepath, subjects2_filepath, monitoring_data_filepath)
else: 
    imaging_filepath = None
    qc_filepath = None
    blood1_filepath = None
    blood2_filepath = None
    subjects1_filepath = None
    subjects2_filepath = None
    monitoring_data_filepath = None

# ----------------------------------------------------------------------------
# LOAD ASSETS FILES
# ----------------------------------------------------------------------------

# Pointers to official files stored at github repository main branch
screening_sites_github_url = 'https://raw.githubusercontent.com/TACC/a2cps-datastore-weekly/main/src/assets/screening_sites.csv'
display_terms_github_url = 'https://raw.githubusercontent.com/TACC/a2cps-datastore-weekly/main/src/assets/A2CPS_display_terms.csv'

# load display terms and screening sites
screening_sites = pd.read_csv(screening_sites_github_url)
display_terms, display_terms_dict, display_terms_dict_multi = load_display_terms_from_github(display_terms_github_url)

# Columns used in reports [UPDATE THIS IF START TO USE MORE]
subjects_raw_cols_for_reports = ['index',
                                'ewcomments',
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
# APIS
# ----------------------------------------------------------------------------
datetime_format = "%m/%d/%Y, %H:%M:%S"
apis_imaging_index = {}
data_state = 'empty'

api_data_index = {
    'blood':'',
    'imaging':'',
    'subjects':'',
    'consort':'',
    'monitoring':'',
}
api_request_state = {
    'blood':None,
    'imaging':None,
    'subjects1':None,
    'subjects2':None,
    'consort':None,
    'monitoring':None,
}
api_data_cache = {
    'blood':None,
    'imaging':None,
    'subjects':None,
    'raw': None,
    'consort':None,
    'monitoring':None,
}

api_data_simple = {
    'blood':None,
    'imaging':None,
    'subjects':None,
    'monitoring':None,
    'raw': None
}

# ----------------------------------------------------------------------------
# APP
# ----------------------------------------------------------------------------

app = Flask(__name__)
app.debug = True
gunicorn_logger = logging.getLogger('gunicorn.error')
app.logger  = logging.getLogger("datastore_app")
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(logging.DEBUG)

logger = logging.getLogger('werkzeug')
logger.addHandler = gunicorn_logger.handlers
logger.setLevel(logging.DEBUG)

@app.before_request
def before_request_log():
    app.logger.debug(f"{request.remote_addr} \"{request.method} {request.url}\"")

@app.after_request
def after_request_log(response):
    app.logger.debug(f"{request.remote_addr} \"{request.method} {request.url}\" {response.status_code}")
    return response

# APIS: try to load new data, if doesn't work, get most recent
@app.route("/api/apis")
def api_apis():
    return jsonify(api_data_index)

@app.route("/api/tester")
def api_tester():
    if local_data_path:
        return jsonify(local_data_path)
    else:
        return jsonify('local_data_path not found')

@app.route("/api/imaging")
def api_imaging():
    global datetime_format
    global api_data_index
    global api_data_cache
    
    try:
        tapis_token = get_tapis_token(request)
        if not api_data_index['imaging'] or not check_data_current(request, datetime.strptime(api_data_index['imaging'], datetime_format)):
            if data_access_type != 'LOCAL':
                data_date = datetime.now().strftime(datetime_format)
                imaging_data = get_api_imaging_data(tapis_token)
            else:
                data_date = local_data_date
                imaging_data = get_local_imaging_data(imaging_filepath, qc_filepath)
            
            if imaging_data:
                api_data_index['imaging'] = data_date
                api_data_cache['imaging'] = imaging_data
                
        return jsonify({'date': api_data_index['imaging'], 'data': api_data_cache['imaging']})
    except Exception as e:
        app.logger.error(("Error in imaging API request: {0}").format(str(e)))
        return jsonify('error: {}'.format(e))

@app.route("/api/consort")
def api_consort():
    global datetime_format
    global api_data_index
    global api_data_cache

    try:
        tapis_token = get_tapis_token(request)
        if not api_data_index['consort'] or not check_data_current(request, datetime.strptime(api_data_index['consort'], datetime_format)):
            api_date = datetime.now().strftime(datetime_format)
            consort_data_json = get_api_consort_data(tapis_token)
            if consort_data_json:
                app.logger.info(f"Caching consort api response data. Date: {api_date}")
                api_data_cache['consort'] = consort_data_json
                api_data_index['consort'] = api_date
        return jsonify({'date': api_data_index['consort'], 'data': api_data_cache['consort']})
    except Exception as e:
        app.logger.error(("Error in consort API request: {0}").format(str(e)))
        return jsonify('error: {}'.format(e))

# get_api_consort_data


@app.route("/api/blood")
def api_blood():
    global datetime_format
    global api_data_index
    global api_data_cache
    try:
        tapis_token = get_tapis_token(request)
        if not api_data_index['blood'] or not check_data_current(request, datetime.strptime(api_data_index['blood'], datetime_format)):

            if data_access_type != 'LOCAL':
                data_date = datetime.now().strftime(datetime_format)
                blood_data, blood_data_request_status = get_api_blood_data(tapis_token)
            else:
                data_date = local_data_date
                blood_data, blood_data_request_status = get_local_blood_data(blood1_filepath, blood2_filepath)
                
            if blood_data:
                app.logger.info(f"Caching blood api response data. Date: {data_date}")
                api_data_index['blood'] = data_date
                api_data_cache['blood'] = blood_data

            with open('requests.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                for i in blood_data_request_status:
                    writer.writerow(i)
                f.close()

        return jsonify({'date': api_data_index['blood'], 'data': api_data_cache['blood']})
    except Exception as e:
        app.logger.error(("Error in blood API request: {0}").format(str(e)))
        return jsonify('error: {}'.format(e))


@app.route("/api/subjects")
def api_subjects():
    global datetime_format
    global api_data_index
    global api_data_cache
    global subjects_raw_cols_for_reports

    try:
        tapis_token = get_tapis_token(request)
        if not api_data_index['subjects'] or not check_data_current(request, datetime.strptime(api_data_index['subjects'], datetime_format)):
            # api_date = datetime.now().strftime(datetime_format)
            if data_access_type != 'LOCAL':
                data_date = datetime.now().strftime(datetime_format)
                latest_subjects_json = get_api_subjects_json(tapis_token)
            else:
                data_date = local_data_date
                latest_subjects_json = get_local_subjects_raw(subjects1_filepath, subjects2_filepath)
                print(latest_subjects_json.keys())

            # if latest_subjects_json:
            latest_data = process_subjects_data(latest_subjects_json,subjects_raw_cols_for_reports,screening_sites, display_terms_dict, display_terms_dict_multi)
            app.logger.info(f"Caching subjects api response data. Date: {data_date}")
            api_data_index['subjects'] = data_date
            api_data_cache['subjects'] = latest_data  
       

        return jsonify({'date': api_data_index['subjects'], 'data': api_data_cache['subjects']})
    except Exception as e:
        app.logger.error(("Error in subjects API request: {0}").format(str(e)))
        return jsonify('error: {}'.format(e))

@app.route("/api/monitoring")
def api_monitoring():
    global datetime_format
    global api_data_index
    global api_data_cache

    try:
        tapis_token = get_tapis_token(request)
        if not api_data_index['monitoring'] or not check_data_current(request, datetime.strptime(api_data_index['monitoring'], datetime_format)):
            # api_date = datetime.now().strftime(datetime_format)
            if data_access_type != 'LOCAL':
                latest_monitoring_json_tuple = get_api_monitoring_data(tapis_token)
            else:
                latest_monitoring_json_tuple = get_local_monitoring_data(monitoring_data_filepath)

            latest_monitoring_json = latest_monitoring_json_tuple[0]
            app.logger.info(latest_monitoring_json.keys())     

            #Convert filename timestamp format "%Y%m%dT%H%M%SZ" to "%m/%d/%Y, %H:%M:%S"
            date_format = "%Y%m%dT%H%M%SZ"
            data_date = latest_monitoring_json['date']
            formatted_date = datetime.strptime(data_date, date_format).strftime("%m/%d/%Y, %H:%M:%S")
            app.logger.info(f"Caching monitoring api response data. Date: {formatted_date}")
            api_data_index['monitoring'] = formatted_date

            api_data_cache['monitoring'] = latest_monitoring_json['data']  

        return jsonify({'date': api_data_index['monitoring'], 'data': api_data_cache['monitoring']})

    
    except Exception as e:
        app.logger.error(("Error in monitoring API request: {0}").format(str(e)))
        return jsonify('error: {}'.format(e))

@app.route("/api/subjects_debug")
def api_subjects_debug():
    global datetime_format
    global api_data_index
    global api_data_cache
    global subjects_raw_cols_for_reports

    try:

        data_date = local_data_date
        latest_subjects_json = get_local_subjects_raw(subjects1_filepath, subjects2_filepath)
        print(latest_subjects_json.keys())

        latest_data = latest_subjects_json
        # latest_data = process_subjects_data(latest_subjects_json,subjects_raw_cols_for_reports,screening_sites, display_terms_dict, display_terms_dict_multi)

        api_data_index['subjects'] = data_date
        api_data_cache['subjects'] = latest_data  


        return jsonify({'date': api_data_index['subjects'], 'data': api_data_cache['subjects']})
    except Exception as e:
        traceback.print_exc()
        return jsonify('error: {}'.format(e))

# @app.route("/api/full")
# def api_full():
#     datafeeds = {}
#     for data_category in api_data_cache:
#         if api_data_cache[data_category]['data']:
#                 datafeeds[data_category] = list(api_data_cache[data_category]['data'].keys())
#         else:
#             datafeeds[data_category] = ['no data']
#     return jsonify(datafeeds)

@app.route("/api/simple")
def api_simple():
    return jsonify({'date':'20231221', 'data':{'test-data':'test-data'}})


if __name__ == "__main__":
    app.run(host='0.0.0.0')