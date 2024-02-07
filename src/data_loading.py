import traceback

import os
import io
import requests
import json

import numpy as np
import pandas as pd
import sqlite3

import datetime
from datetime import datetime

import logging
logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------
# ENV Variables
# ----------------------------------------------------------------------------
data_access_type = os.environ.get('DATA_ACCESS_TYPE')
files_api_root = os.environ.get('FILES_API_ROOT') 
portal_api_root = os.environ.get('PORTAL_API_ROOT')

# LOCAL DATA SETTINGS
local_data_path = os.environ.get("LOCAL_DATA_PATH","")
local_data_date = os.environ.get("LOCAL_DATA_DATE","2022-01-01")

current_folder = os.path.dirname(__file__)
DATA_PATH = os.path.join(current_folder,local_data_path)
ASSETS_PATH = os.path.join(current_folder,'assets')


# ----------------------------------------------------------------------------
# Updating data checks
# ----------------------------------------------------------------------------
def check_data_current(data_date):
    '''test to see if the date in a data dictionary is from after 10am on the same day as checking.'''
    now = datetime.now()

    if data_date.date() == now.date():
        if data_date.hour < 15 and datetime.now().hour >= 15:
            return False
        else:
            return True
    else:
        return False

def check_available_data(available_data):
    if available_data:
        if isinstance(available_data, list) and type(available_data[-1]) is dict and 'date' in available_data[-1].keys() and 'data' in available_data[-1].keys():
            latest_date = available_data[-1]['date']
            if isinstance(pd.to_datetime(latest_date), datetime):
                if check_data_current(pd.to_datetime(latest_date)):
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False
    else:
        return False


# ----------------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------------

def use_b_if_not_a(a, b):
    if not pd.isnull(a):
        x = a
    else:
        x = b
    return x

def dict_to_col(df, index_cols, dict_col, new_col_name = 'category', add_col_as_category=True):
    ''' Take a dataframe with index columns and a column containing a dictionary and convert
    the dictionary json into separate columns'''
    new_df = df[index_cols +[dict_col]].copy()
    new_df.dropna(subset=[dict_col], inplace=True)
    new_df.reset_index(inplace=True, drop=True)
    if add_col_as_category:
        new_df[new_col_name] = dict_col
    new_df = pd.concat([new_df, pd.json_normalize(new_df[dict_col])], axis=1)
    return new_df

def move_column_inplace(df, col, pos):
    ''' move a column position in df'''
    col = df.pop(col)
    df.insert(pos, col.name, col)

# ----------------------------------------------------------------------------
# DATA DISPLAY DICTIONARIES
# ----------------------------------------------------------------------------

def get_display_dictionary(display_terms, api_field, api_value, display_col):
    '''from a dataframe with the table display information, create a dictionary by field to match the database
    value to a value for use in the UI '''
    try:
        display_terms_list = display_terms[api_field].unique() # List of fields with matching display terms

        # Create a dictionary using the field as the key, and the dataframe to map database values to display text as the value
        display_terms_dict = {}
        for i in display_terms_list:
            term_df = display_terms[display_terms.api_field == i]
            term_df = term_df[[api_value,display_col]]
            term_df = term_df.rename(columns={api_value: i, display_col: i + '_display'})
            term_df = term_df.apply(pd.to_numeric, errors='ignore')
            display_terms_dict[i] = term_df
        return display_terms_dict

    except Exception as e:
        traceback.print_exc()
        return None


def load_display_terms_from_github(display_terms_gihub_raw_url):
    '''Load the data file that explains how to translate the data columns and controlled terms into the English language
    terms to be displayed to the user'''
    try:
        display_terms = pd.read_csv(display_terms_gihub_raw_url)

        # Get display terms dictionary for one-to-one records
        display_terms_uni = display_terms[display_terms.multi == 0]
        display_terms_dict = get_display_dictionary(display_terms_uni, 'api_field', 'api_value', 'display_text')

        # Get display terms dictionary for one-to-many records
        display_terms_multi = display_terms[display_terms.multi == 1]
        display_terms_dict_multi = get_display_dictionary(display_terms_multi, 'api_field', 'api_value', 'display_text')

        return display_terms, display_terms_dict, display_terms_dict_multi
    except Exception as e:
        traceback.print_exc()
        return None

# ----------------------------------------------------------------------------
# LOAD DATA FROM LOCAL FILES, Return *JSON*
# ----------------------------------------------------------------------------

def get_local_imaging_data(imaging_filepath, qc_filepath):
    ''' Load data from local imaging files. '''
    try:
        imaging = pd.read_csv(imaging_filepath)
        qc = pd.read_csv(qc_filepath)

        imaging_data_json = {
            'imaging' : imaging.to_dict('records'),
            'qc' : qc.to_dict('records')
        }

        return imaging_data_json

    except Exception as e:
        traceback.print_exc()
        return {'status': 'Problem with local imaging files'}

def get_local_subjects_raw(subjects1_filepath, subjects2_filepath):
    ''' Load subjects data from local files'''
    print(subjects1_filepath, subjects2_filepath)
    try:
        with open(subjects1_filepath) as file1:
            subjects1 = json.load(file1)

        with open(subjects2_filepath) as file2:
            subjects2 = json.load(file2)

         # Create combined json
        subjects_json = {'1': subjects1, '2': subjects2}

        return subjects_json

    except Exception as e:
        traceback.print_exc()
        return {'Stats': 'Subjects data not available'}


def get_local_blood_data(blood1_filepath, blood2_filepath):
    ''' Load blood data from local files'''

    try:
        blood_json = {}

        with open(blood1_filepath) as file:
            blood1 = json.load(file)

        with open(blood2_filepath) as file:
            blood2 = json.load(file)

         # Create combined json
        blood_json = {'1': blood1, '2': blood2}

        blood = bloodjson_to_df(blood_json, ['1','2'])
        blood = simplify_blooddata(blood)

        blood_data_json = {
            'blood' : blood.to_dict('records')
        }

        request_status = ['local file']

        return blood_data_json, request_status

    except Exception as e:
        traceback.print_exc()
        return {'Stats': 'Blood data not available'}


def get_local_monitoring_data(monitoring_data_filepath):
    ''' Load monitoring data from local files'''

    try:
        with open(monitoring_data_filepath) as file:
            monitoring_json = json.load(file)

            request_status = ['local file']

        return monitoring_json, request_status

    except Exception as e:
        traceback.print_exc()
        return {'Stats': 'Monitoring data not available'}

# ----------------------------------------------------------------------------
# LOAD DATA FROM API
# ----------------------------------------------------------------------------
# Get Tapis token if authorized to access data files
def get_tapis_token(api_request):
    try:
        response = requests.get(portal_api_root + '/auth/tapis/', cookies=api_request.cookies)
                                #headers={'cookie':'coresessionid=' + api_request.cookies.get('coresessionid')})
        if response:
            tapis_token = response.json()['token']
            return tapis_token
        else:
            logger.warning("Unauthorized to access tapis token")
            raise Exception
    except Exception as e:
        logger.warning('portal api error: {}'.format(e))
        return False

def get_api_consort_data(api_request,
                        report='consort', 
                        report_suffix = 'consort-data-[mcc]-latest.csv'):
    '''Load data for a specified consort file. Handle 500 server errors'''
    try:
        tapis_token = get_tapis_token(api_request)

        if tapis_token:
            cosort_columns = ['source','target','value', 'mcc']
            consort_df = pd.DataFrame(columns=cosort_columns)

            # # get list of mcc files
            # filename1 = report_suffix.replace('[mcc]',str(1))
            # filename2 = report_suffix.replace('[mcc]',str(2))
            # files_list = [filename1, filename2]

       
            mcc_list = [1,2]
            for mcc in mcc_list:
                filename = report_suffix.replace('[mcc]',str(mcc))
                csv_url = '/'.join([files_api_root, report, filename])
                csv_request = requests.get(csv_url, headers={'X-Tapis-Token': tapis_token})
                csv_content = csv_request.content
                try:
                    csv_df = pd.read_csv(io.StringIO(csv_content.decode('utf-8')), usecols=[0,1,2], header=None)
                    csv_df['mcc'] = mcc
                    csv_df.columns = cosort_columns
                except:
                    csv_df = pd.DataFrame(columns=cosort_columns)
                consort_df = pd.concat([consort_df,csv_df])

            consort_dict = consort_df.to_dict('records')
            if not consort_dict:
                consort_dict = ['No data found']
            # IF DATA LOADS SUCCESSFULLY:
            consort_data_json = {
                'consort' : consort_df.to_dict('records')
            }
            return consort_data_json
        
        else:
            logger.warning("Unauthorized attempt to access Consort data")
            return None

    except Exception as e:
        traceback.print_exc()
        return None

## Function to rebuild dataset from apis

def get_api_imaging_data(api_request):
    ''' Load data from imaging api. Return bad status notice if hits Tapis API'''
    try:       
        tapis_token = get_tapis_token(api_request)

        if tapis_token:
            # IMAGING
            imaging_filepath = '/'.join([files_api_root,'imaging','imaging-log-latest.csv'])
            imaging_request = requests.get(imaging_filepath, headers={'X-Tapis-Token': tapis_token})
            if imaging_request.status_code == 200:
                imaging = pd.read_csv(io.StringIO(imaging_request.content.decode('utf-8')))
            else:
                return {'status':'500', 'source': 'imaging-log-latest.csv'}


            qc_filepath = '/'.join([files_api_root,'imaging','qc-log-latest.csv'])
            qc_request = requests.get(qc_filepath, headers={'X-Tapis-Token': tapis_token})
            if qc_request.status_code == 200:
                qc = pd.read_csv(io.StringIO(qc_request.content.decode('utf-8')))
            else:
                return {'status':'500', 'source': 'qc-log-latest.csv'}

            # IF DATA LOADS SUCCESSFULLY:
            imaging_data_json = {
                'imaging' : imaging.to_dict('records'),
                'qc' : qc.to_dict('records')
            }

            return imaging_data_json
        else:
            logger.warning("Unauthorized attempt to access Imaging data")
            return None

    except Exception as e:
        traceback.print_exc()
        return "exception: {}".format(e)
    
## Monitoring data for Briha's app
def get_api_monitoring_data(api_request):
    ''' Load monitoring data from api'''
    try:      
        current_datetime = datetime.now()
        tapis_token = get_tapis_token(api_request)
        
        if tapis_token:    
            # Monitoring
            monitoring_filepath = '/'.join([files_api_root,'data-monitoring','aggregated.json'])
            monitoring_request = requests.get(monitoring_filepath, headers={'X-Tapis-Token': tapis_token})


            if monitoring_request.status_code == 200:
                monitoring_request_status = [current_datetime.strftime("%m/%d/%Y, %H:%M:%S"), monitoring_filepath, '200']
                monitoring_data_json = monitoring_request.json()
            else:
                monitoring_request_status = [current_datetime.strftime("%m/%d/%Y, %H:%M:%S"), monitoring_filepath, monitoring_request.status_code ]
                monitoring_data_json = None

            return monitoring_data_json, monitoring_request_status
        else:
            logger.warning("Unauthorized attempt to access Monitoring data")
            return None

    except Exception as e:
        traceback.print_exc()
        return None    

## Function to rebuild dataset from apis
def get_api_blood_data(api_request):
    ''' Load blood data from api'''
    try:      
        current_datetime = datetime.now()
        tapis_token = get_tapis_token(api_request)
        
        if tapis_token:    
            # BLOOD
            blood1_filepath = '/'.join([files_api_root,'blood','blood-1-latest.json'])
            blood1_request = requests.get(blood1_filepath, headers={'X-Tapis-Token': tapis_token})

            blood2_filepath = '/'.join([files_api_root,'blood','blood-2-latest.json'])
            blood2_request = requests.get(blood2_filepath, headers={'X-Tapis-Token': tapis_token})

            if blood1_request.status_code == 200:
                blood1 = blood1_request.json()
                blood1_request_status = [current_datetime.strftime("%m/%d/%Y, %H:%M:%S"), blood1_filepath, '200']
            else:
                blood1_request_status = [current_datetime.strftime("%m/%d/%Y, %H:%M:%S"), blood1_filepath, blood1_request.status_code ]
                # return None, {'status':'500', 'source': api_dict['blood']['blood1']}

            if blood2_request.status_code == 200:
                blood2 = blood2_request.json()
                blood2_request_status = [current_datetime.strftime("%m/%d/%Y, %H:%M:%S"), blood2_filepath, '200']
            else:
                blood2_request_status = [current_datetime.strftime("%m/%d/%Y, %H:%M:%S"), blood2_filepath, blood2_request.status_code]
                # return None, {'date' : 'status':'500', 'source': api_dict['blood']['blood2']}

            if blood1_request.status_code == 200 and blood2_request.status_code == 200:
                blood_json = {'1': blood1, '2': blood2}

                blood = bloodjson_to_df(blood_json, ['1','2'])
                blood = simplify_blooddata(blood)

                blood_data_json = {
                    'blood' : blood.to_dict('records')
                }
            else:
                blood_data_json = None

            request_status = [blood1_request_status, blood2_request_status]

            return blood_data_json, request_status
        else:
            logger.warning("Unauthorized attempt to access Blood data")
            return None

    except Exception as e:
        traceback.print_exc()
        return None
       

def get_api_subjects_json(api_request):
    ''' Load subjects data from api. Note data needs to be cleaned, etc. to create properly formatted data product'''
    try:        
        tapis_token = get_tapis_token(api_request)

        if tapis_token:
            # Load Json Data
            subjects1_filepath = '/'.join([files_api_root,'subjects','subjects-1-latest.json'])
            subjects1_request = requests.get(subjects1_filepath, headers={'X-Tapis-Token': tapis_token})
            if subjects1_request.status_code == 200:
                subjects1 = subjects1_request.json()
            else:
                return None
                # return {'status':'500', 'source': api_dict['subjects']['subjects1']}

            subjects2_filepath = '/'.join([files_api_root,'subjects','subjects-2-latest.json'])
            subjects2_request = requests.get(subjects2_filepath, headers={'X-Tapis-Token': tapis_token})
            if subjects2_request.status_code == 200:
                subjects2 = subjects2_request.json()
            else:
                return None
                # return {'status':'500', 'source': api_dict['subjects']['subjects2']}

            # Create combined json
            subjects_json = {'1': subjects1, '2': subjects2}

            return subjects_json
        else:
            logger.warning("Unauthorized attempt to access Subjects data")
            return None

    except Exception as e:
        traceback.print_exc()
        return None


# ----------------------------------------------------------------------------
# PROCESS SUBJECTS DATA
# ----------------------------------------------------------------------------

# 1. Combine separate jsons for each MCC into a single data frame
def combine_mcc_json(mcc_json):
    '''Convert MCC json subjects data into dataframe and combine'''
    df = pd.DataFrame()
    for mcc in mcc_json.keys():
        mcc_data = pd.DataFrame.from_dict(mcc_json[mcc], orient='index')
        mcc_data['mcc'] = mcc
        print(mcc + str(len(mcc_data)))
        if df.empty:
            print('df empty')
            df = mcc_data
            print('empty df len: '+ str(len(df)))
        else:
            print('df len: '+ str(len(df)))
            df = pd.concat([df, mcc_data],axis=0)
            
            print('df len post concat: '+ str(len(df)))
    return df

# 2. extract nested 1-to-many adverse events data
def extract_adverse_effects_data(subjects_data, adverse_effects_col = 'adverse_effects'):
    '''Extract data with multiple values (stored as 'adverse effects' column) from the subjects data.
    Adverse effects data is stored in a nested dictionary format - this function unpacks that.'''

    index_cols = ['index','main_record_id', 'mcc']
    # reset index using index_cols
    multi_data = subjects_data.set_index(index_cols).copy()
    # Extract multi data values
    multi_df = multi_data[[adverse_effects_col]].dropna()
    # Convert from data frame back to dict
    multi_dict = multi_df.to_dict('index')
    # Turn dict into df with multi=index and reset_index
    multi = pd.DataFrame.from_dict({(i,k): multi_dict[i][j][k]
                               for i in multi_dict.keys()
                               for j in multi_dict[i].keys()
                               for k in multi_dict[i][j].keys()
                           },
                           orient='index')
    # Replace empty strings with NaN
    multi = multi.replace(r'^\s*$', np.nan, regex=True)
    multi = multi.reset_index()
    # Convert level 0 of index from nested index back into columns
    multi[index_cols] = pd.DataFrame(multi['level_0'].tolist(), index=multi.index)
    # Label level 1 of multiindex as the instance of adverse events for a given subject
    multi['instance'] = multi['level_1']
    # Drop the extraneous columns
    multi.drop(['level_0', 'level_1'], axis=1, inplace=True)

    # Move index columns to start of dataframe
    index_cols.append('instance')
    new_col_order = index_cols + list(multi.columns.drop(index_cols))
    multi = multi[new_col_order]
    return multi

# 3. Clean subjects dataframe
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

    df = df.merge(sites, how='left', on=id_col)

    return df

def create_clean_subjects(subjects_raw, screening_sites, display_terms_dict, display_terms_dict_multi, drop_cols_list =['adverse_effects']):
    '''Take the raw subjects data frame and clean it up. Note that apis don't pass datetime columns well, so
    these should be converted to datetime by the receiver.
    datetime columns = ['date_of_contact','date_and_time','obtain_date','ewdateterm','sp_surg_date','sp_v1_preop_date','sp_v2_6wk_date','sp_v3_3mo_date']
    Can convert within a pd.DataFrame using .apply(pd.to_datetime, errors='coerce')'''
    try:
        #--- Clean up subjects (move to own function?)
        subjects = subjects_raw.copy()
        # Rename 'index' to 'record_id'
        subjects.rename(columns={"index": "record_id"}, inplace = True)

        # Drop adverse events column
        subjects = subjects.drop(columns=drop_cols_list)
        # Convert all string 'N/A' values to nan values
        subjects = subjects.replace('N/A', np.nan)

        # Handle 1-many dem_race, take multi-select values and convert to 8
        if not np.issubdtype(subjects['dem_race'].dtype, np.number):
            subjects['dem_race_original'] = subjects['dem_race']
            subjects.loc[(subjects.dem_race.str.contains('|', regex=False, na=False)),'dem_race']='8'

        # Coerce numeric values to enable merge
        subjects = subjects.apply(pd.to_numeric, errors='ignore')

        # Merge columns on the display terms dictionary to convert from database terminology to user terminology
        for i in display_terms_dict.keys():
            if i in subjects.columns: # Merge columns if the column exists in the dataframe
                display_terms = display_terms_dict[i]
                if subjects[i].dtype == np.float64:
                    # for display columns where data is numeric, merge on display dictionary, treating cols as floats to handle nas
                    display_terms[i] = display_terms[i].astype('float64')
                subjects = subjects.merge(display_terms, how='left', on=i)
        #------

        # Add screening sites
        subjects = add_screening_site(screening_sites, subjects, 'record_id')

        # Convert datetime columns
        datetime_cols_list = ['date_of_contact','date_and_time','obtain_date','ewdateterm','sp_surg_date','sp_v1_preop_date','sp_v2_6wk_date','sp_v3_3mo_date'] #erep_local_dtime also dates, but currently an array
        subjects[datetime_cols_list] = subjects[datetime_cols_list].apply(pd.to_datetime, errors='coerce')

        return subjects

    except Exception as e:
        traceback.print_exc()
        return None

# 4. Extract data for only those patients who ultimately consented
def get_consented_subjects(subjects_with_screening_site):
    '''Get the consented patients from subjects dataframe with screening sites added'''
    consented = subjects_with_screening_site[subjects_with_screening_site.obtain_date.notnull()].copy()
    consented['treatment_site'] = consented.apply(lambda x: use_b_if_not_a(x['sp_data_site_display'], x['redcap_data_access_group_display']), axis=1)
    consented['treatment_site_type'] = consented['treatment_site'] + "/" + consented['surgery_type']

    return consented


# 5. restrict Adverse Events data to those subjects identified as 'consented' in step 5
def clean_adverse_events(adverse_events, consented, display_terms_dict_multi):
    try:
        # Coerce to numeric
        multi_data = adverse_events.apply(pd.to_numeric, errors='ignore')

        # Convert numeric values to display values using dictionary
        for i in display_terms_dict_multi.keys():
            if i in multi_data.columns:
                multi_data = multi_data.merge(display_terms_dict_multi[i], how='left', on=i)
        # Rename 'index' to 'record_id'
        multi_data.rename(columns={"index": "record_id"}, inplace = True)

        # merge with consented data to get treatment_site column
        multi_data = consented[['record_id','treatment_site']].copy().merge(multi_data, how='right', on='record_id')

        return multi_data
    except Exception as e:
        traceback.print_exc()
        return None

def convert_datetime_to_isoformat(df, datetime_cols_list):
    # Convert Datetime to isoformat for API
    for col in datetime_cols_list:
        df[col] = df[col].apply(lambda x: x.isoformat())
    return df

def process_subjects_data(subjects_raw_json, subjects_raw_cols_for_reports,screening_sites, display_terms_dict, display_terms_dict_multi):
    ''' Take the raw subjects json and process it into separate, cleaned dataframes for subjects, consented subjects and adverse events'''
    # 1. Combine separate jsons for each MCC into a single data frame
    subjects_raw = combine_mcc_json(subjects_raw_json)
    subjects_raw.reset_index(inplace=True)
    # print('raw: ' + str(len(subjects_raw)))

    # 3. extract nested 1-to-many adverse events data
    adverse_effects_raw = extract_adverse_effects_data(subjects_raw)

    # 4. Limit subjects to columns needed for reports
    subjects = subjects_raw[subjects_raw_cols_for_reports].copy()

    # 5. Clean subjects dataframe
    subjects = create_clean_subjects(subjects_raw, screening_sites, display_terms_dict, display_terms_dict_multi)
    # print('subjects: ' + str(len(subjects)))

    # 6. Extract data for only those patients who ultimately consented
    consented = get_consented_subjects(subjects)
    # print('consented: ' + str(len(consented)))

    # 7. restrict Adverse Events data to those subjects identified as 'consented' in step 5
    adverse_events = clean_adverse_events(adverse_effects_raw, consented, display_terms_dict_multi)

    # 8. Convert datetime cols to isoformat for API
    datetime_cols_list = ['date_of_contact','date_and_time','obtain_date','ewdateterm','sp_surg_date','sp_v1_preop_date','sp_v2_6wk_date','sp_v3_3mo_date']
    subjects = convert_datetime_to_isoformat(subjects, datetime_cols_list)
    consented = convert_datetime_to_isoformat(consented, datetime_cols_list)

    subjects_api_data ={
            'subjects_cleaned': subjects.to_dict('records'),
            'consented': consented.to_dict('records'),
            'adverse_events': adverse_events.to_dict('records')
    }
    return subjects_api_data

# ----------------------------------------------------------------------------
# PROCESS BLOOD DATA
# ----------------------------------------------------------------------------

# 1. Blood JSON input into Dataframe
def bloodjson_to_df(json, mcc_list):
    df = pd.DataFrame()
    dict_cols = ['Baseline Visit', '6-Wks Post-Op', '3-Mo Post-Op']
    for mcc in mcc_list:
        if mcc in json.keys():
            m = json[mcc]
        if str(mcc) in json.keys():
            mcc=str(mcc)
            m = json[mcc]
        if m:
            mdf = pd.DataFrame.from_dict(m, orient='index')
            mdf.dropna(subset=['screening_site'], inplace=True)
            mdf.reset_index(inplace=True)
            mdf['MCC'] = mcc
            for c in dict_cols:
                if c in mdf.columns:
                    col_df = dict_to_col(mdf, ['index','MCC','screening_site'], c,'Visit')
                    df = pd.concat([df, col_df])
                    df.reset_index(inplace=True, drop=True)
    return df

# 2. Clean blood dataframe
def simplify_blooddata(blood_df):
    '''Take the raw blood data frame and simplify by dropping columns with the nested dictionaries,
    and moving visit column to beginning of dataframe.'''

    # Drop baseline dict, 6 week dict, 3 month dict
    blood_df.drop(['Baseline Visit', '6-Wks Post-Op', '3-Mo Post-Op'], axis=1, inplace=True)

    # move Visit column to beginning of DF
    move_column_inplace(blood_df, 'Visit', 2)

    return blood_df

def clean_blooddata(blood_df):
    '''Take the raw subjects data frame and clean it up. Note that apis don't pass datetime columns well, so
    these should be converted to datetime by the receiver.
    datetime columns = ['date_of_contact','date_and_time','obtain_date','ewdateterm','sp_surg_date','sp_v1_preop_date','sp_v2_6wk_date','sp_v3_3mo_date']
    Can convert within a pd.DataFrame using .apply(pd.to_datetime, errors='coerce')'''

    # Convert numeric columns
    numeric_cols = ['bscp_aliq_cnt','bscp_protocol_dev','bscp_protocol_dev_reason']
    blood_df[numeric_cols] = blood_df[numeric_cols].apply(pd.to_numeric,errors='coerce')

    # Convert datetime columns
    datetime_cols = ['bscp_time_blood_draw','bscp_aliquot_freezer_time','bscp_time_centrifuge']
    blood_df[datetime_cols] = blood_df[datetime_cols].apply(pd.to_datetime,errors='coerce')

    # Add calculated columns
    # Calculate time to freezer: freezer time - blood draw time
    blood_df['time_to_freezer'] = blood_df['bscp_aliquot_freezer_time'] - blood_df['bscp_time_blood_draw']
    blood_df['time_to_freezer_minutes'] = blood_df['time_to_freezer'].dt.components['hours']*60 + blood_df['time_to_freezer'].dt.components['minutes']

    # Calculate time to centrifuge: centrifuge time - blood draw time
    blood_df['time_to_centrifuge'] = blood_df['bscp_time_centrifuge'] - blood_df['bscp_time_blood_draw']
    blood_df['time_to_centrifuge_minutes'] = blood_df['time_to_centrifuge'].dt.components['hours']*60 + blood_df['time_to_centrifuge'].dt.components['minutes']

    # Calculate times exist in correct order
    blood_df['time_values_check'] = (blood_df['time_to_centrifuge_minutes'] < blood_df['time_to_freezer_minutes'] ) & (blood_df['time_to_centrifuge_minutes'] <= 30) & (blood_df['time_to_freezer_minutes'] <= 60)

    # Get 'Site' column that combines MCC and screening site
    blood_df['Site'] = 'MCC' + blood_df['MCC'].astype(str) + ': ' + blood_df['screening_site']

    # Convert Deviation Numeric Values to Text
    deviation_dict = {1:'Unable to obtain blood sample -technical reason',
                      2: 'Unable to obtain blood sample -patient related',
                      3: 'Sample handling/processing error'}
    deviation_df = pd.DataFrame.from_dict(deviation_dict, orient='index')
    deviation_df.reset_index(inplace=True)
    deviation_df.columns = ['bscp_protocol_dev_reason','Deviation Reason']
    blood_df = blood_df.merge(deviation_df, on='bscp_protocol_dev_reason', how='left')

    # Clean column names for more human friendly usage
    rename_dict = {'index':'ID',
                   'screening_site':'Screening Site',
                   'bscp_deg_of_hemolysis':'Hemolysis'}

    # rename index col as ID
    blood_df = blood_df.rename(columns=rename_dict)

    return blood_df

# ----------------------------------------------------------------------------
# OTHER APIS?
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# GENERATE DICTIONARIES FOR API OUTPUTS (using functions above)
# ----------------------------------------------------------------------------
