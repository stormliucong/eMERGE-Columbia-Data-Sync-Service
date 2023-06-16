from distutils.command.config import config
import requests
import json
from datetime import datetime
import pandas as pd
import logging
import argparse
import numpy as np
   
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

def export_data_from_redcap(api_key : str, api_endpoint : str, is_local_record : bool = False) -> list:
    '''
    Export data from REDCap using API
    Input: api_key: API token
           api_endpoint: api endpoint url
           id_only: whether to export only id fields
    Output: data: a json object containing all the data from REDCap
    '''
    logging.info(f"Exporting data from {api_endpoint}...")
    if is_local_record:
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
        'fields[0]': 'first_name',
        'fields[1]': 'last_name',
        'fields[2]': 'date_of_birth',
        'fields[3]': 'age',
        'fields[4]': 'first_name_child',
        'fields[5]': 'last_name_child',
        'fields[6]': 'date_of_birth_child',
        'fields[7]': 'participant_lab_id',
        'fields[8]': 'last_update_timestamp',
        'fields[9]': 'record_id',
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
   
def indexing_local_data(data: list) -> pd.DataFrame:
    '''
    Indexing local data for matching
    Input: local_data: a dictionary containing all the local data
    Output: local_data_df: a dataframe containing all the local data
    '''
    logging.info("Indexing local dataset...")
    if local_data != []:
        df = pd.DataFrame(data)
        # identify the fields for mapping purpose in local data
        # patch 6/6 match by child and parent seperatedly.
        local_data_df = df[['cuimc_id','first_local','last_local','dob','child_first','last_child','dob_child', 'mrn', 'cuimc_empi', 'record_id', 'participant_lab_id','age']] 
        local_data_df = local_data_df.apply(lambda x: x.str.strip().str.lower() if x.dtype == "object" else x)
        local_data_df['dob_child'] = pd.to_datetime(local_data_df['dob_child'], format='%Y-%m-%d')
        local_data_df['dob'] = pd.to_datetime(local_data_df['dob'], format='%Y-%m-%d')
        local_data_df = local_data_df[local_data_df['first_local']!='']
        local_data_df['cuimc_id'] = local_data_df['cuimc_id'].astype(int)
    else:
        local_data_df = pd.DataFrame()
    return local_data_df

def indexing_r4_data(data: list) -> pd.DataFrame:
    '''
    indexing R4 data
    Input: r4_data: a list of json objects containing R4 data
    Output: r4_data_df: a pandas dataframe containing R4 data
    '''
    logging.info("Indexing R4 dataset...")
    if r4_data != []:
        df = pd.DataFrame(data)
        r4_data_df = df[['record_id','first_name','last_name','date_of_birth','age','first_name_child','last_name_child','date_of_birth_child']]
        # get first row.
        r4_data_df = r4_data_df.groupby('record_id').first().reset_index()
        r4_data_df = r4_data_df.apply(lambda x: x.str.strip().str.lower() if x.dtype == "object" else x)
        r4_data_df['age'] = r4_data_df['age'].apply(lambda x: 999 if x == '' else int(x)) # if age not generated match as adult.
    else:
        r4_data_df = pd.DataFrame()
    return r4_data_df

def find_duplicates(query_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Find duplicates in target_df based on query_df
    Input: query_df: a pandas dataframe containing the query data
           target_df: a pandas dataframe containing the target data
    Output: duplicates_df: a pandas dataframe containing the duplicates
    '''
    logging.info("Checking duplicates...")
    # Step 1: map by dob and name
    duplicates_df = query_df.merge(target_df, how='inner', left_on=['dob','first_local','last_local'], right_on=['date_of_birth','first_name','last_name'])
    r4_data_unmapped_df = query_df[query_df['age']>=18].merge(target_df, how='inner', left_on=['dob','first_local','last_local'], right_on=['date_of_birth','first_name','last_name'])
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
    r4_data_unmapped_df['cuimc_id'] = range(current_mapping['cuimc_id'].max()+1, current_mapping['cuimc_id'].max()+1+len(r4_data_unmapped_df))
    current_mapping = pd.concat([current_mapping, r4_data_unmapped_df])
    # Step 6. There is a few participants might have multiple records in R4, therefore one cuimc id can have multiple record_ids. 
    # Select the most recent record_id as the current record_id for that cuimc id
    # Some record_id has been removed from R4 due to participant withdraw
    current_mapping = current_mapping.merge(r4_data_df[['record_id','last_update_timestamp']].drop_duplicates(), on='record_id')
    current_mapping['last_update_timestamp'] = pd.to_datetime(current_mapping['last_update_timestamp'], format='%Y-%m-%d %H:%M:%S')
    current_mapping = current_mapping.loc[current_mapping.groupby('cuimc_id')['last_update_timestamp'].idxmax()]
    current_mapping = current_mapping[['record_id','cuimc_id']]
    return current_mapping
           
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, required=False, help="file to write log",)    
    parser.add_argument('--token', type=str, required=False,  help='json file with api tokens')
    parser.add_argument('--output_prefix', type=str, required=False, help='prefix of output files')
    args = parser.parse_args()

    # if token file is not provided, use the default token file
    if args.token is None:
        token_file = './api_tokens.json'
    else:
        token_file = args.token
    
    # if log file is not provided, use the default log file
    if args.log is None:
        log_file = './extract_id_mapping.log'
    else:
        log_file = args.log

    # if output prefix is not provided, use the default output prefix
    if args.output_prefix is None:
        output_prefix = './test'
    else:
        output_prefix = args.output_prefix
    
    # set up logging.
    logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    logging.info('Start program...')
    # logging.basicConfig(level=logging.INFO)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

    api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint = read_api_config(config_file = token_file)
    r4_data = export_data_from_redcap(api_key_r4,r4_api_endpoint, is_local_record=False)
    if r4_data != []:
        r4_data_df = indexing_r4_data(r4_data)
        local_data = export_data_from_redcap(api_key_local,cu_local_endpoint, is_local_record=True)
        local_data_df = indexing_local_data(local_data)
        current_mapping = match_r4_local_data(r4_data_df, local_data_df)
        current_mapping.to_csv(output_prefix + '_current_mapping.csv', index=False)
        local_data_df.to_csv(output_prefix + '_local_df.csv',index=False)
        r4_data_df.to_csv(output_prefix + '_r4_df.csv',index=False)
        r4_mapping = r4_data_df.merge(current_mapping)
        local_data_df['record_id_local_r4'] = local_data_df['record_id']
        local_data_df.drop(columns=['record_id'],inplace=True)
        master_df = r4_mapping.merge(local_data_df)
        master_notna_df = master_df[(master_df['first_local'].notna() & master_df['first_name'].notna())]
        master_notna_df[master_notna_df['first_local']!=master_notna_df['first_name']].to_csv(output_prefix + '_name_mismatch_df.csv',index=False)
    logging.info('End program...')
