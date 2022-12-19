# A2CPS DATASTORE
## Sample `docker-compose` With Datastore
This application provides a data store container for A2CPS data to serve the reports

### Container
The `datastore` is a container accessible only within the Docker network that exposes a `/api` endpoint and returns json data

### Running
You can run this application with `docker-compose up`.

# DATA PROCESSING
## SUBJECTS
Subjects data comes as 2 json streams: 1 each for MCC1 and MCC2. These are combined and then cleaned into the necessary data frames

### Basic Subjects Processing

The subjects apis are received as separate streams of raw data from each MCC.  The below describes the steps used to process this data, and the functions used to carry out these steps.  For further details on specific functions, they are found in the data_processing.py file

| Step | Functions   |
|:------|:------|
|1. Load jsons for both MCCs | call API |
|2. Combine separate jsons for each MCC into a single data frame | combine_mcc_json|
|3. Extract nested 1-to-many data (Adverse Events)  | extract_adverse_effects_data|
|4. Limit columns to those used in report | N/A - restrict to report columns list defined as an input parameter. list subject to change during report development|
|5. Clean subjects dataframe| add_screening_site, create_clean_subjects|
|6. Extract data for only those patients who ultimately consented | get_consented_subjects|    
|7. restrict Adverse Events data to those subjects identified as 'consented' in step 6 | clean_adverse_events|    
|8. Convert datetime cols to isoformat for API | convert_datetime_to_isoformat |   
