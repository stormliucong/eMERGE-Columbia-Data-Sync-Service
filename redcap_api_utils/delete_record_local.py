import requests
import json
import argparse
import pandas as pd
import logging
from datetime import datetime



def read_api_config(config_file):
    api_token_file = config_file
    with open(api_token_file,'r') as f:
        api_conf = json.load(f)

    api_key_local = api_conf['api_key_local'] # API token for local
    api_key_r4 = api_conf['api_key_r4'] # API token for R4.
    cu_local_endpoint = api_conf['local_endpoint'] # local api endpoint
    r4_api_endpoint = api_conf['r4_api_endpoint'] # R4 api endpoint
    return api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint

def delete_record(record_id,api_key_local,cu_local_endpoint):
    data = {
        'token': api_key_local,
        'action': 'delete',
        'content': 'record',
        'records[0]': str(record_id),
    }
    r = requests.post(cu_local_endpoint,data=data)
    logging.debug('HTTP Status: ' + str(r.status_code))
    logging.debug(r.text)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, required=False, help="file to write log",)    
    parser.add_argument('--token', type=str, required=False,  help='json file with api tokens')
    parser.add_argument('--delete_id_df', type=str, required=True, help='Path to the csv file containing the record ids to be deleted')
    args = parser.parse_args()
    
     # if token file is not provided, use the default token file
    if args.token is None:
        token_file = '../api_tokens.json'
    else:
        token_file = args.token
    
    # if log file is not provided, use the default log file
    if args.log is None:
        log_file = './delete_records.log'
    else:
        log_file = args.log
    # set up logging.
    logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

    logging.info('Start program...')
    # logging.basicConfig(level=logging.INFO)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    logging.info('Start program...')

    delete_id_df = pd.read_csv(args.delete_id_df)
    record_id_list = delete_id_df['cuimc_id'].unique().tolist()
    api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint = read_api_config(config_file=token_file)
    for record_id in record_id_list:
        delete_record(record_id,api_key_local,cu_local_endpoint)
    logging.info('End program...')
    
