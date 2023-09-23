import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# Suppress the InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json
from datetime import datetime
import pandas as pd
import warnings
# Suppress the FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)
import logging
import argparse
import os
import smtplib
from email.message import EmailMessage
import sys
from data_pull_from_r4 import read_api_config



def export_file_from_redcap(api_key : str, api_endpoint : str, record_id = None) -> list:
    file_fields = ["metree_import_json_file","gira_pdf"]
    for ff in file_fields:
        # get file name 
        data = {
            'token': api_key,
            'content': 'record',
            'action': 'export',
            'format': 'json',
            'type': 'flat',
            'csvDelimiter': '',
            'fields[0]': 'record_id',
            'fields[1]': ff,
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'false',
            'exportDataAccessGroups': 'false',
            'returnFormat': 'json'
            }
        if record_id is not None:
            data['records[0]'] = record_id
        r = requests.post(api_endpoint,data=data)
        if r.json() != []:
            return_name_list = r.json()
            for return_name in return_name_list:
                logging.debug('Return name: {}'.format(return_name))
                record_id = return_name['record_id']
                file_name = return_name[ff]
                if file_name != '':
                    # whether it is exist in the file_repo
                    if os.path.exists(os.path.join('file_repo', file_name)):
                        logging.debug('File {} for record {} is already downloaded.'.format(file_name, record_id))
                    else:
                        # download the file
                        data = {
                            'token': api_key,
                            'content': 'file',
                            'action': 'export',
                            'record': record_id,
                            'field': ff,
                            'event': '',
                            'returnFormat': 'json'
                            }
                        r = requests.post(api_endpoint,data=data)
                        file_bytes = r.content
                        with open(os.path.join('file_repo', file_name), 'wb') as f:
                            f.write(file_bytes)
                        logging.debug('File {} for record {} is downloaded.'.format(file_name, record_id))
                        
    return return_name_list       
       
    

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--log_folder', type=str, required=False, help="folder to write log",)    
        parser.add_argument('--token', type=str, required=False,  help='json file with api tokens')   
        parser.add_argument('--r4_id', type=int, required=False, help="r4 id for a single participant sync")    
        args = parser.parse_args()

        # if token file is not provided, use the default token file
        if args.token is None:
            token_file = '../api_tokens.json'
        else:
            token_file = args.token
        
        # if log file is not provided, use the default log file
        date_string = datetime.now().strftime("%Y%m%d")
        if args.log_folder is None:
            log_file = 'logs/file_pull_from_r4_' + date_string + '.log'
        else:
            log_file = os.path.join(args.log_folder, 'data_pull_from_r4_' + date_string + '.log')
        
        if args.r4_id is not None:
            r4_id = str(args.r4_id)
        else:
            r4_id = None
            
         # set up logging.
        logging.basicConfig(filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

        logging.info('Start pulling file from R4...')
        
        api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint = read_api_config(config_file = token_file)
        # for testing purpose
        r4_id = None
        return_name_list = export_file_from_redcap(api_key_r4,r4_api_endpoint, record_id=r4_id)
        
        logging.info('Finished pulling file from R4.')
    
    except Exception as e:
        logging.error('Error: {}'.format(e))
        sys.exit(1)