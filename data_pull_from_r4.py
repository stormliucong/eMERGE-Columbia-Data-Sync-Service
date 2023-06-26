from distutils.command.config import config
import requests
import json
from datetime import datetime
import pandas as pd
import logging
import argparse
import numpy as np

def read_redcap_fields_from_record(api_key: str, api_endpoint : str) -> list:
    '''
    Read REDCap fields from REDCap using API
    Input: api_key: API token
           api_endpoint: api endpoint url
    Output: field_name_list: a list of field names
    '''

    # local Data dictionary export
    logging.info("Reading REDCAP fields from "  + str(api_endpoint) + "...")
    data = {
            'token': api_key,
            'content': 'record',
            'action': 'export',
            'format': 'json',
            'type': 'flat',
            'csvDelimiter': '',
            'rawOrLabel': 'raw',
            'records[0]': '1',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'false',
            'exportDataAccessGroups': 'false',
            'returnFormat': 'json'
        }
    r = requests.post(api_endpoint,data=data)
    record = r.json()[0]
    field_name_list = record.keys()
    return field_name_list

def read_ignore_fields(ignore_file: str) -> list:
    '''
    Read ignore fields from ignore_R4_fields.json
    Input: ignore_file: path to ignore_R4_fields.json
    Output: ignore_fields: a list of fields to be ignored
    '''
    logging.info("Reading ignore fields...")
    # read json
    with open(ignore_file,'r') as f:
        ignore_fields = json.load(f)
        # iterative dictionary 
        ignore_fields = [k for k, v in ignore_fields.items() if str(v) == '1']
        logging.info("Ignore fields: " + ','.join(ignore_fields))
    return ignore_fields    

def read_api_config(config_file: str = './api_tokens.json') -> tuple:
    '''
    Read api tokens and endpoint url from api_config.json
    Input: config_file: path to api_config.json
    Output: api_key_local: API token for local
            api_key_r4: API token for R4.
            cu_local_endpoint: local api endpoint
            r4_api_endpoint: R4 api endpoint
    '''
    logging.info("Reading api tokens and endpoint url...")
    api_token_file = config_file
    with open(api_token_file,'r') as f:
        api_conf = json.load(f)

    api_key_local = api_conf['api_key_local'] # API token for local
    api_key_r4 = api_conf['api_key_r4'] # API token for R4.
    cu_local_endpoint = api_conf['local_endpoint'] # local api endpoint
    r4_api_endpoint = api_conf['r4_api_endpoint'] # R4 api endpoint
    return api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint

def export_data_from_redcap(api_key : str, api_endpoint : str, id_only : bool = False) -> list:
    '''
    Export data from REDCap using API
    Input: api_key: API token
           api_endpoint: api endpoint url
           id_only: whether to export only id fields
    Output: data: a json object containing all the data from REDCap
    '''
    logging.info(f"Exporting data from {api_endpoint}...")
    if id_only:
        data = {
        'token': api_key,
        'content': 'record',
        'action': 'export',
        'format': 'json',
        'type': 'flat',
        'fields[0]': 'last_local',
        'fields[1]': 'first_local',
        'fields[2]': 'dob',
        'fields[3]': 'last_child',
        'fields[4]': 'child_first',
        'fields[5]': 'dob_child',
        'fields[6]': 'mrn',
        'fields[7]': 'cuimc_id',
        'fields[8]': 'cuimc_empi',
        'fields[9]': 'last_name',
        'fields[10]': 'first_name',
        'fields[11]': 'date_of_birth',
        'fields[12]': 'last_name_child',
        'fields[13]': 'first_name_child',
        'fields[14]': 'date_of_birth_child',
        'fields[15]': 'participant_lab_id',
        'fields[16]': 'age',
        'fields[17]': 'record_id',
        'csvDelimiter': '',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    else:
        data = {
            'token': api_key,
            'content': 'record',
            'action': 'export',
            'format': 'json',
            'type': 'flat',
            'csvDelimiter': '',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'false',
            'exportDataAccessGroups': 'false',
            'returnFormat': 'json'
        }
    flag = 1
    while(flag > 0 and flag < 5):
        try:
            r = requests.post(api_endpoint,data=data)
            if r.status_code == 200:
                logging.debug('HTTP Status: ' + str(r.status_code))
                data = r.json()
                return data
            else:
                logging.error('Error occured in exporting data from ' + api_endpoint)
                logging.error('HTTP Status: ' + str(r.status_code))
                logging.error(r.content)
                flag = flag + 1
        except Exception as e:
            logging.error('Error occured in exporting data. ' + str(e))
            flag = 5
    return {}

def export_survey_queue_link(record_id : str, api_key : str, api_endpoint: str) -> str:
    '''
    Export survey queue link from REDCap using API
    Input: record_id: the record id of the participant
           api_key: API token
           api_endpoint: api endpoint url
    Output: return_url: a json object containing surveyQueueLink from REDCap
    '''
    data = {
        'token': api_key,
        'content': 'surveyQueueLink',
        'action': 'export',
        'format': 'json',
        'returnFormat': 'json',
        'record' : record_id
    }
    flag = 1
    while(flag > 0 and flag < 5):
        try:
            r = requests.post(api_endpoint,data=data)
            if r.status_code == 200:
                flag = 0
                return_url = r.content.decode("utf-8") 
                return return_url
            else:
                logging.error('Error occured in exporting survey queue link.')
                logging.error('HTTP Status: ' + str(r.status_code) + '. R4 record_id: ' + record_id)
                logging.error(r.content)
                flag = flag + 1
        except Exception as e:
            logging.error('Error occured in exporting survey queue link. ' + str(e))
            logging.error('R4 record_id: ' + record_id)
            flag = 5
    return ""
     
def indexing_local_data(local_data: list) -> pd.DataFrame:
    '''
    Indexing local data for matching
    Input: local_data: a dictionary containing all the local data
    Output: local_data_df: a dataframe containing all the local data
    '''
    logging.info("Indexing local dataset...")
    if local_data != []:
        local_data_df = pd.DataFrame(local_data)
        # identify the fields for mapping purpose in local data
        # patch 6/6 match by child and parent seperatedly.
        local_data_df = local_data_df[['cuimc_id','first_local','last_local','dob','last_child','child_first','dob_child','participant_lab_id','record_id']] 
        local_data_df = local_data_df.apply(lambda x: x.str.strip().str.lower() if x.dtype == "object" else x)
        local_data_df['cuimc_id'] = local_data_df['cuimc_id'].astype(int)

    else:
        local_data_df = pd.DataFrame()
    return local_data_df

def indexing_r4_data(r4_data: list) -> pd.DataFrame:
    '''
    indexing R4 data
    Input: r4_data: a list of json objects containing R4 data
    Output: r4_data_df: a pandas dataframe containing R4 data
    '''
    logging.info("Indexing R4 dataset...")
    if r4_data != []:
        r4_data_df = pd.DataFrame(r4_data)
        r4_data_df = r4_data_df[['record_id','first_name','last_name','date_of_birth','age','first_name_child','last_name_child','date_of_birth_child','participant_lab_id','last_update_timestamp']]
        # get first row.
        r4_data_df = r4_data_df.groupby('record_id').first().reset_index()
        r4_data_df = r4_data_df.apply(lambda x: x.str.strip().str.lower() if x.dtype == "object" else x)
        r4_data_df['age'] = r4_data_df['age'].apply(lambda x: 999 if x == '' else int(x)) # if age not generated match as adult.
    else:
        r4_data_df = pd.DataFrame()
    return r4_data_df

def match_r4_local_data(r4_data_df : pd.DataFrame, local_data_df : pd.DataFrame) -> pd.DataFrame:
    '''
    Match R4 and local data
    Input: r4_data_df: a pandas dataframe containing R4 data
           local_data_df: a pandas dataframe containing local data
    Output: current_mapping: a pandas dataframe containing matched data
    '''
    logging.info("Matching R4 and local dataset...")
    # Step 1: identify old cuimc_id and record_id mapping
    current_mapping = local_data_df[local_data_df['record_id']!=''][['record_id','cuimc_id']].drop_duplicates()
    # due to historical reason, some record_id is not unique, one record can somehow mapped to multiple cuimc_ids
    # Step 2: map by participant_lab_id previously pulled from R4
    r4_data_unmapped_df = r4_data_df[~r4_data_df['record_id'].isin(current_mapping['record_id'])][['record_id','participant_lab_id']].drop_duplicates()
    qualified_local_df = local_data_df[local_data_df['participant_lab_id']!=''][['cuimc_id','participant_lab_id']].drop_duplicates()
    current_mapping = pd.concat([current_mapping, qualified_local_df.merge(r4_data_unmapped_df)[['record_id','cuimc_id']].drop_duplicates()])
    # Step 3: map by name and dob for adult
    r4_data_unmapped_df = r4_data_df[~r4_data_df['record_id'].isin(current_mapping['record_id'])][['record_id','first_name','last_name','date_of_birth','age']].drop_duplicates()
    r4_data_unmapped_df = r4_data_unmapped_df[r4_data_unmapped_df['age']>=18]
    qualified_local_df = local_data_df[(local_data_df['first_local'] != '') & (local_data_df['last_local'] != '') & (local_data_df['dob'] != '')][['cuimc_id','first_local','last_local','dob','child_first']].drop_duplicates()
    qualified_local_df = qualified_local_df[~qualified_local_df['child_first'].str.upper().str.isupper()][['cuimc_id','first_local','last_local','dob']].drop_duplicates()
    current_mapping = pd.concat([current_mapping, qualified_local_df.merge(r4_data_unmapped_df,left_on=['first_local','last_local','dob'], right_on=['first_name','last_name','date_of_birth'])[['record_id','cuimc_id']].drop_duplicates()])
    # Step 4 map by name and dob for child
    r4_data_unmapped_df = r4_data_df[~r4_data_df['record_id'].isin(current_mapping['record_id'])][['record_id','first_name_child','last_name_child','date_of_birth_child','age']].drop_duplicates()
    r4_data_unmapped_df = r4_data_unmapped_df[r4_data_unmapped_df['age']<18]
    qualified_local_df = local_data_df[(local_data_df['child_first'] != '') & (local_data_df['last_child'] != '') & (local_data_df['dob_child'] != '')][['cuimc_id','child_first','last_child','dob_child']].drop_duplicates()
    qualified_local_df = qualified_local_df[qualified_local_df['child_first'].str.upper().str.isupper()][['cuimc_id','child_first','last_child','dob_child']].drop_duplicates()
    current_mapping = pd.concat([current_mapping, qualified_local_df.merge(r4_data_unmapped_df,left_on=['child_first','last_child','dob_child'], right_on=['first_name_child','last_name_child','date_of_birth_child'])[['record_id','cuimc_id']].drop_duplicates()])
    # Step 5 auto generate cuimc id
    r4_data_unmapped_df = r4_data_df[~r4_data_df['record_id'].isin(current_mapping['record_id'])][['record_id']].drop_duplicates()
    newly_created_cuimc_id_start = local_data_df['cuimc_id'].max() + 1
    newly_created_cuimc_id_end = local_data_df['cuimc_id'].max()+1+len(r4_data_unmapped_df)
    logging.info(f"Newly created cuimc id range: {newly_created_cuimc_id_start} - {newly_created_cuimc_id_end}")
    r4_data_unmapped_df['cuimc_id'] = range(newly_created_cuimc_id_start, newly_created_cuimc_id_end)
    current_mapping = pd.concat([current_mapping, r4_data_unmapped_df])
    # Step 6. There is a few participants might have multiple records in R4, therefore one cuimc id can have multiple record_ids. 
    # Select the most recent record_id as the current record_id for that cuimc id
    # Some record_id has been removed from R4 due to participant withdraw
    current_mapping = current_mapping.merge(r4_data_df[['record_id','last_update_timestamp']].drop_duplicates(), on='record_id')
    current_mapping['last_update_timestamp'] = pd.to_datetime(current_mapping['last_update_timestamp'], format='%Y-%m-%d %H:%M:%S')
    current_mapping = current_mapping.loc[current_mapping.groupby('cuimc_id')['last_update_timestamp'].idxmax()]
    current_mapping = current_mapping[['record_id','cuimc_id']]
    return current_mapping

def push_data_to_local(api_key_local: str, cu_local_endpoint: str, push_to_local_list : list) -> int:
    '''
    Push data to local REDCap
    Input: api_key_local: API key for local REDCap
           cu_local_endpoint: API endpoint for local REDCap
           push_to_local_list: list of records to push to local REDCap
    Output: 1 if success, 0 if failure
    '''
    logging.info('Push to local REDCap...')
    data = {
        'token': api_key_local,
        'content': 'record',
        'action': 'import',
        'format': 'json',
        'type': 'flat',
        'overwriteBehavior': 'overwrite',
        'forceAutoNumber': 'false',
        'data': json.dumps(push_to_local_list),
        'returnContent': 'count',
        'returnFormat': 'json'
    }
    flag = 1
    while(flag > 0 and flag < 3):
        
        r = requests.post(cu_local_endpoint,data=data)
        if r.status_code == 200:
            logging.debug('HTTP Status: ' + str(r.status_code))
            if 'ERROR' in str(r.content):
                logging.error(str(r.content))
                logging.error('No record updated')
                return 0
            else:
                logging.info('Updated records: ' + str(r.content))
                return 1
        else:
            logging.error('Error occured in importing data to ' + cu_local_endpoint)
            logging.error('HTTP Status: ' + str(r.status_code))
            logging.error(r.content)
            flag = flag + 1
    
    return 0

def get_r4_links(api_key: str, api_endpoint : str, current_mapping: pd.DataFrame, r4_data: list) -> pd.DataFrame:
    '''
    Get the survey queue link for each participant in the current_mapping
    Input: api_key: API key for R4
           api_endpoint: API endpoint for R4
           current_mapping: the current mapping between R4 and local REDCap
    Output: current_mapping with an additional column for the survey queue link
    '''
    # record_id_list = current_mapping[['record_id']].drop_duplicates()
    logging.info("Getting survey queue link...")

    r4_data_df = pd.DataFrame(r4_data)
    non_repeat_instance_df = r4_data_df[r4_data_df['redcap_repeat_instrument']=='']

    r4_return_urls_df = non_repeat_instance_df[['record_id','survey_queue_link']].drop_duplicates()
    r4_return_urls_df['r4_survey_queue_link'] = r4_return_urls_df['survey_queue_link']
    
    # def export_survey_queue_link_wrapper(r4_record):
    #     # Additional parameter
    #     return export_survey_queue_link(r4_record, api_key=api_key,api_endpoint=api_endpoint)
    
    # Apply the wrapper function to the array
    # return_urls = np.apply_along_axis(export_survey_queue_link_wrapper, axis=0, arr=np.array(record_id_list)) # does not work may be due to syncronization issue?
    # return_urls = list(map(export_survey_queue_link_wrapper, record_id_list)) # very slow but works

    # Combine the arrays into a DataFrame
    # return_urls_df = pd.DataFrame({'r4_survey_queue_link': return_urls, 'record_id': record_id_list})
    # Merge the DataFrames
    current_mapping = pd.merge(current_mapping, r4_return_urls_df, on='record_id',how='left')
    # drop the survey_queue_link column
    current_mapping = current_mapping.drop('survey_queue_link', axis=1)
    return current_mapping

def prepare_local_list(current_mapping : pd.DataFrame, r4_data : list, ignore_fields : list, local_fields: list, current_time : str) -> list:
    '''
    Prepare the list to push to local REDCap
    Input: current_mapping: the current mapping between R4 and local REDCap
           r4_data: the data pulled from R4
           ignore_fields: the fields to ignore
           current_time: current time
    Output: the list to push to local REDCap
    '''
    logging.info("Preparing Pushing list...")
    current_mapping_df = get_r4_links(api_key_r4,r4_api_endpoint,current_mapping, r4_data)
    current_mapping_df['last_r4_pull'] = current_time
     # check is it a redcap_repeat_instrument
    r4_data_df = pd.DataFrame(r4_data)
    r4_data_df = r4_data_df.drop(ignore_fields, axis=1)
    r4_data_df = r4_data_df.merge(current_mapping_df)
    repeat_instance_df = r4_data_df[r4_data_df['redcap_repeat_instrument']!='']
    repeat_instance_df = repeat_instance_df.copy()
    repeat_instance_df.drop(['record_id','r4_survey_queue_link','last_r4_pull'],axis=1,inplace=True)
    non_repeat_instance_df = r4_data_df[r4_data_df['redcap_repeat_instrument']=='']
    r4_data_df = pd.concat([non_repeat_instance_df, repeat_instance_df])
    r4_data_df.fillna('', inplace=True)
    more_ignore_fields = [i for i in r4_data_df.columns if i not in local_fields]
    logging.info("More_ignore_fields...")
    logging.info(more_ignore_fields)
    r4_data_df.drop(more_ignore_fields, axis=1, inplace=True)
    push_to_local_list = r4_data_df.to_dict(orient='records')
    return push_to_local_list
            
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, required=False, help="file to write log",)    
    parser.add_argument('--token', type=str, required=False,  help='json file with api tokens')   
    parser.add_argument('--ignore', type=str, required=False, help="json file with ignored R4 fields")    
    args = parser.parse_args()

    # if token file is not provided, use the default token file
    if args.token is None:
        token_file = './api_tokens.json'
    else:
        token_file = args.token
    
    # if ignore file is not provided, use the default ignore file
    if args.ignore is None:
        ignore_file = './ignore_R4_fields.json'
    else:
        ignore_file = args.ignore

    # if log file is not provided, use the default log file
    if args.log is None:
        log_file = './data_pull_from_r4.log'
    else:
        log_file = args.log
    
    # set up logging.
    logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    logging.info('Start pulling data from R4...')
    # logging.basicConfig(level=logging.INFO)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

    api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint = read_api_config(config_file = token_file)
    ignore_fields = read_ignore_fields(ignore_file = ignore_file)
    local_fields = read_redcap_fields_from_record(api_key_local, cu_local_endpoint)
    r4_data = export_data_from_redcap(api_key_r4,r4_api_endpoint, id_only=False)
    if r4_data != []:
        r4_data_df = indexing_r4_data(r4_data)
        local_data = export_data_from_redcap(api_key_local,cu_local_endpoint, id_only=True)
        local_data_df = indexing_local_data(local_data)
        current_mapping = match_r4_local_data(r4_data_df, local_data_df)
        current_mapping.to_csv('./test_current_mapping.csv', index=False)
        push_to_local_list = prepare_local_list(current_mapping, r4_data, ignore_fields, local_fields, dt_string)
        # Define the batch size
        batch_size = 250
        # Iterate over the list in batches
        for i in range(0, len(push_to_local_list), batch_size):
            logging.info(f"Index: {i}...Pushing data to local REDCap...")
            batch = push_to_local_list[i:i+batch_size]
            status = push_data_to_local(api_key_local, cu_local_endpoint, batch)
            if status == 1:
                logging.info(f"Index: {i}...Data pull from R4 is successful")
            else:
                logging.error(f"Index: {i}...Data pull from R4 is not successful")
    logging.info('End pulling data from R4...')
