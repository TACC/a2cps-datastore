from data_loading import *
import logging

logger  = logging.getLogger("datastore_app")

# ----------------------------------------------------------------------------
# input processing
# ----------------------------------------------------------------------------

def subset_imaging_data(imaging_full):
    ''' Clean up the incoming imaging dataframe to only columns actually used'''
    imaging_columns_used = ['site', 'subject_id', 'visit','acquisition_week','Surgery Week','bids', 'dicom', 
    'T1 Indicated','DWI Indicated', '1st Resting State Indicated','fMRI Individualized Pressure Indicated', 
    'fMRI Standard Pressure Indicated','2nd Resting State Indicated',
    'T1 Received', 'DWI Received', 'fMRI Individualized Pressure Received', 'fMRI Standard Pressure Received',
    '1st Resting State Received', '2nd Resting State Received','Cuff1 Applied Pressure']

    imaging = imaging_full[imaging_columns_used].copy() # Select subset of columns
    imaging = imaging.replace('na', np.nan) # Clean up data
    imaging['completions_id'] = imaging.apply(lambda x: str(x['subject_id']) + x['visit'],axis=1) # Add completions id value from combination of subject ID and visit

    return imaging

def subset_qc_data(qc_full):
    ''' Subset QC dataframe'''
    qc_cols_used = ['site', 'sub', 'ses', 'scan','rating']
    qc = qc_full[qc_cols_used].copy() # Select subset of columns    
    # Set columns to categorical values
    ignore = ['sub']
    qc = (qc.set_index(ignore, append=True)
            .astype("category")
            .reset_index(ignore)
           )
    
    return qc

# generate_raw_data_dict
# TO DO: Remove categorical and put that in to Imaging report b/c no json serializable
def generate_imaging_report_data_dictionary(data_source, version, data_date, imaging_df, qc_df):
    ''' Create a complete data dictionary for use in the imaging report using dataframes as input'''
    
    imaging_df = subset_imaging_data(imaging_df)
    qc_df = subset_qc_data(qc_df)

    imaging_dtypes_dict = {
        "site":"category",
        "visit":"category",
        }   
    imaging_df = imaging_df.astype(dtype= imaging_dtypes_dict)
    sites = imaging_df.site.unique()

    qc_dtypes_dict = {
        "site":"category",
        "ses":"category",
        "scan":"category",
        "rating":"category",
        }
    qc_df = qc_df.astype(dtype= qc_dtypes_dict)
    qc_df["rating"] = qc_df["rating"].cat.add_categories("unavailable")

    full_qc = generate_missing_qc(imaging_df, qc_df)

    imaging_report_data_dictionary = {
        'version': version,
        'date': data_date,
        'imaging': imaging_df.to_dict('records'),
        'qc': full_qc.to_dict('records'),
        'imaging_source': data_source,
        'qc_source': data_source,
        'sites': sites,
    }

    return imaging_report_data_dictionary

# ----------------------------------------------------------------------------
# LOCAL DATA
# ----------------------------------------------------------------------------
def get_local_imaging_releases(imaging_release_filepath):
    ''' Load imaging release data from release files. '''
    try:
        with open(imaging_release_filepath) as imaging_file:
            imaging_releases = json.load(imaging_file)

        return imaging_releases

    except Exception as e:
        traceback.print_exc()
        return {'Stats': 'Error obtaining Imaging release data'}

def get_local_imaging_data(imaging_filepath, qc_filepath):
    ''' Load data from local imaging files. '''
    try:
        imaging_full = pd.read_csv(imaging_filepath)
        imaging = subset_imaging_data(imaging_full)

        qc_full = pd.read_csv(qc_filepath)
        qc = subset_qc_data(qc_full)

        imaging_data_json = {
            'imaging' : imaging.to_dict('records'),
            'qc' : qc.to_dict('records')
        }

        return imaging_data_json

    except Exception as e:
        traceback.print_exc()
        return {'status': 'Problem with local imaging files'}

# ----------------------------------------------------------------------------
# CORRAL DATA WITH TAPIS
# ----------------------------------------------------------------------------

def get_api_imaging_releases(tapis_token):
    ''' Load imaging release data from corral files. '''
    try:
        if tapis_token:
            # IMAGING RELEASES FILEPATH
            imaging_releases_filepath = '/'.join([files_api_root,'data-products','imaging_releases.json'])
            imaging_releases_request = make_report_data_request(imaging_releases_filepath, tapis_token)
            logger.info('imaging_releases_request')
            logger.info(imaging_releases_request)
            if imaging_releases_request.status_code == 200:
                imaging_releases = imaging_releases_request.json()
                logger.info('imaging_releases')
                logger.info(imaging_releases[:2])
            else:
                logger.info('return none')
                return None
            return imaging_releases
        else:
           raise TapisTokenRetrievalException()

    except Exception as e:
        traceback.print_exc()
        return "exception: {}".format(e)

def get_api_imaging_data(tapis_token):
    ''' Load data from imaging api. Return bad status notice if hits Tapis API'''
    try:
        if tapis_token:
            # IMAGING
            imaging_filepath = '/'.join([files_api_root,'imaging','imaging-log-latest.csv'])
            imaging_request = make_report_data_request(imaging_filepath, tapis_token)
            if imaging_request.status_code == 200:
                imaging_full = pd.read_csv(io.StringIO(imaging_request.content.decode('utf-8')))
                imaging = subset_imaging_data(imaging_full)
            else:
                return {'status':'500', 'source': 'imaging-log-latest.csv'}


            qc_filepath = '/'.join([files_api_root,'imaging','qc-log-latest.csv'])
            qc_request = make_report_data_request(qc_filepath, tapis_token)
            if qc_request.status_code == 200:
                qc_full = pd.read_csv(io.StringIO(qc_request.content.decode('utf-8')))
                qc = subset_qc_data(qc_full)
            else:
                return {'status':'500', 'source': 'qc-log-latest.csv'}

            # IF DATA LOADS SUCCESSFULLY:
            imaging_data_json = {
                'imaging' : imaging.to_dict('records'),
                'qc' : qc.to_dict('records')
            }

            return imaging_data_json
        else:
           raise TapisTokenRetrievalException()

    except Exception as e:
        traceback.print_exc()
        return "exception: {}".format(e)
    