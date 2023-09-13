import requests
import json
from datetime import datetime
import pandas as pd
import logging
import argparse
   
def read_api_config(config_file: str = '../api_tokens.json') -> tuple:
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
        'fields[18]': 'rec_outcome',
        'fields[19]': 'rec_outcome_2',
        'fields[29]': 'rec_outcome_3',
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
        
        # patch 09/13 clear child name if 'nan'
        df['child_first'] = df['child_first'].apply(lambda x: '' if x == 'nan' else x)
        df['last_child'] = df['last_child'].apply(lambda x: '' if x == 'nan' else x)
        local_data_df = df[['cuimc_id','first_local','last_local','dob','child_first','last_child','dob_child', 'mrn', 'cuimc_empi', 'record_id', 'participant_lab_id','age','rec_outcome','rec_outcome_2','rec_outcome_3']] 
        local_data_df = local_data_df.apply(lambda x: x.str.strip().str.lower() if x.dtype == "object" else x)
        local_data_df['dob_child'] = pd.to_datetime(local_data_df['dob_child'], format='mixed', errors='coerce')
        local_data_df['dob'] = pd.to_datetime(local_data_df['dob'], format='mixed', errors='coerce')
        local_data_df = local_data_df[local_data_df['first_local']!='']
        local_data_df['cuimc_id'] = local_data_df['cuimc_id'].astype(int)
    else:
        local_data_df = pd.DataFrame()
    return local_data_df

def find_duplicates(query_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Find duplicates in target_df based on query_df
    Input: query_df: a pandas dataframe containing the query data
    Output: duplicates_df: a pandas dataframe containing the duplicates
    '''
    logging.info("Checking duplicates...")
    # Step 1: group by 'dob','first_local','last_local', 'child_first', 'last_child', 'dob_child' and get count
    # Step 2: filter out the records with count > 1
    # Step 3: merge with target_df to get the duplicates cuimc_ids
    # Step 4 sort by dob, first_local, last_local, child_first, last_child, dob_child
    logging.debug(f"Checking duplicates in {query_df.shape[0]} records.")
    duplicates_df = query_df[query_df['child_first'] == '']
    duplicates_df = duplicates_df.groupby(['dob','first_local','last_local'])['cuimc_id'].nunique().reset_index()
    logging.debug(f"Checking duplicates in {duplicates_df.shape[0]} records after group by.")
    duplicates_df = duplicates_df[duplicates_df['cuimc_id']>1]
    logging.debug(f"Found {duplicates_df.shape[0]} records with duplicates or child.")
    duplicates_df = duplicates_df[['dob','first_local','last_local']]
    duplicates_df = duplicates_df.merge(query_df, how='inner', left_on=['dob','first_local','last_local'], right_on=['dob','first_local','last_local'])    
    duplicates_df = duplicates_df[['dob','first_local','last_local','child_first', 'last_child', 'dob_child', 'cuimc_id','record_id','rec_outcome','rec_outcome_2','rec_outcome_3']]
    duplicates_df = duplicates_df.sort_values(by=['dob','first_local','last_local', 'child_first', 'last_child', 'dob_child'])
    logging.info(f"Found {duplicates_df.shape[0]} duplicates.")
    return duplicates_df

def de_duplicates(duplicates_df):
    '''
    if there is a R4 ID don't de-duplicate
    if there is no R4 IDs, de-duplicate based on the following rules:
        1. if declined, keep the record with the declined status
        2. if not declined, keep the record with the largest cuimc id
    '''
    R4_id_available_df = duplicates_df[duplicates_df['record_id']!='']
    R4_id_available_df = R4_id_available_df[['dob','first_local','last_local']].merge(duplicates_df, how='inner', left_on=['dob','first_local','last_local'], right_on=['dob','first_local','last_local'])
    R4_id_available_df = R4_id_available_df.drop_duplicates()
    R4_id_available_ids = R4_id_available_df['cuimc_id'].unique().tolist() # keep both records in R4 ids is available in either record
    
    R4_id_not_available_df = duplicates_df[~duplicates_df['cuimc_id'].isin(R4_id_available_ids)]                 
    declined_1_df = R4_id_not_available_df[(R4_id_not_available_df['rec_outcome']=='9' )]
    declined_2_df = R4_id_not_available_df[(R4_id_not_available_df['rec_outcome_2']=='9' )]
    declined_3_df = R4_id_not_available_df[(R4_id_not_available_df['rec_outcome_3']=='9' )]
    declined_ids = declined_1_df['cuimc_id'].unique().tolist() + declined_2_df['cuimc_id'].unique().tolist() + declined_3_df['cuimc_id'].unique().tolist() # only keep the declined records
    declined_df = R4_id_not_available_df[R4_id_not_available_df['cuimc_id'].isin(declined_ids)]
    declined_duplicates_df = declined_df[['dob','first_local','last_local']].drop_duplicates().merge(R4_id_not_available_df, how='inner', left_on=['dob','first_local','last_local'], right_on=['dob','first_local','last_local'])
    logging.debug(declined_df.columns)
    declined_duplicates_ids = declined_duplicates_df['cuimc_id'].unique().tolist() # keep both records if declined
    not_declined_df = R4_id_not_available_df[~(R4_id_not_available_df['cuimc_id'].isin(declined_duplicates_ids))]
    # select the one with largest cuimc id
    not_declined_largest_cuimc_id_df = not_declined_df.sort_values(by=['cuimc_id'], ascending=False).groupby(['dob','first_local','last_local']).head(1)
    declined_largest_cuimc_id_df = declined_df.sort_values(by=['cuimc_id'], ascending=False).groupby(['dob','first_local','last_local']).head(1)
    cuimc_id_not_delete = not_declined_largest_cuimc_id_df['cuimc_id'].unique().tolist() + declined_largest_cuimc_id_df['cuimc_id'].unique().tolist() + R4_id_available_ids
    # after_dedup_df = duplicates_df[duplicates_df['cuimc_id'].isin(cuimc_id_not_delete)]
    delete_df = duplicates_df[~duplicates_df['cuimc_id'].isin(cuimc_id_not_delete)]
    not_to_delete_df = duplicates_df[duplicates_df['cuimc_id'].isin(cuimc_id_not_delete)]
    return R4_id_available_df,  declined_df, delete_df, not_to_delete_df

    
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, required=False, help="file to write log",)    
    parser.add_argument('--token', type=str, required=False,  help='json file with api tokens')
    parser.add_argument('--output_prefix', type=str, required=False, help='prefix of output files')
    args = parser.parse_args()

    # if token file is not provided, use the default token file
    if args.token is None:
        token_file = '../api_tokens.json'
    else:
        token_file = args.token
    
    # if log file is not provided, use the default log file
    if args.log is None:
        log_file = './duplicates_marker.log'
    else:
        log_file = args.log

    # if output prefix is not provided, use the default output prefix
    if args.output_prefix is None:
        output_prefix = './test'
    else:
        output_prefix = args.output_prefix
    
    # set up logging.
    logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

    logging.info('Start program...')
    # logging.basicConfig(level=logging.INFO)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

    api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint = read_api_config(config_file = token_file)
    local_data = export_data_from_redcap(api_key_local,cu_local_endpoint, is_local_record=True)
    local_data_df = indexing_local_data(local_data)
    duplicates_df = find_duplicates(local_data_df)
    R4_id_available_df,  declined_df, delete_df, not_to_delete_df = de_duplicates(duplicates_df)
    declined_df.to_csv(output_prefix + '_declined.csv', index=False)
    R4_id_available_df.to_csv(output_prefix + '_R4_id_available.csv', index=False)
    delete_df.to_csv(output_prefix + '_to_delete.csv', index=False)
    not_to_delete_df.to_csv(output_prefix + '_not_to_delete.csv', index=False)
    logging.info('End program...')
