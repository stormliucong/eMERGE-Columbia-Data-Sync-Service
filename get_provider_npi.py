from distutils.command.config import config
import requests
import json
from datetime import datetime
import pandas as pd
import logging
import argparse
import copy
import re

def read_api_config(config_file):
    logging.info("reading api tokens and endpoint url...")
    api_token_file = config_file
    with open(api_token_file,'r') as f:
        api_conf = json.load(f)

    api_key_local = api_conf['api_key_local'] # API token for local
    api_key_r4 = api_conf['api_key_r4'] # API token for R4.
    cu_local_endpoint = api_conf['local_endpoint'] # local api endpoint
    r4_api_endpoint = api_conf['r4_api_endpoint'] # R4 api endpoint
    return api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint

def export_data_from_redcap(api_key, api_endpoint):
    logging.info("export data from " + api_endpoint)
    data = {
        'token': api_key,
        'content': 'record',
        'action': 'export',
        'format': 'json',
        'type': 'flat',
        'csvDelimiter': '',
        'fields[0]': 'cuimc_id',
        'fields[1]': 'provider_name',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    flag = 1
    while(flag > 0 and flag < 5):
        r = requests.post(api_endpoint,data=data)
        if r.status_code == 200:
            logging.info('HTTP Status: ' + str(r.status_code))
            data = r.json()
            return data
        else:
            logging.error('Error occured in exporting data from ' + api_endpoint)
            logging.error('HTTP Status: ' + str(r.status_code))
            logging.error(r.content)
            flag = flag + 1
    return None

def parse_names(provider_name):
    tokens =  re.split(',|\s+|\.',provider_name)
    tokens = [token for token in tokens if token != '']
    if tokens[0].lower() == 'dr.' or tokens[0].lower() == 'dr' or tokens[0].lower() == 'doctor' or tokens[0].lower() == 'dra.' or tokens[0].lower() == 'dra':
        if len(tokens) == 4:
            # skip middle name
            return tokens[1], tokens[3]
        if len(tokens) == 3:
            return tokens[1], tokens[2]
        if len(tokens) == 2:
            # only last name
            return '', tokens[1]
    elif tokens[len(tokens)-1].strip().lower() == ',md' or tokens[len(tokens)-1].strip().lower() == 'md' or tokens[len(tokens)-1].strip().lower() == ',np' or tokens[len(tokens)-1].strip().lower() == 'np':
        if len(tokens) == 4:
            # skip middle name
            return tokens[0], tokens[2]
        if len(tokens) == 3:
            return tokens[0], tokens[1]
        if len(tokens) == 2:
            # only last name
            return '', tokens[0]
    else:    
        if len(tokens) == 2:
            return tokens[0], tokens[1]
        if len(tokens) == 3:
            # skip middle name
            return tokens[0], tokens[2]
    return '', ''

def get_provider_api(first_name, last_name):
    npi = 0
    try:
        # provider_name = record['provider_name']
        # first_name = provider_name.split(' ')[0].strip()
        # last_name = provider_name.split(' ')[1].strip()
        NPPES_NPI_Registry_endpoint = "https://npiregistry.cms.hhs.gov/api"
        # first_name = 'Wendy'
        # last_name = 'Makkawi'
        params = {
            'number' : '',
            'enumeration_type' : '',
            'taxonomy_description' : '',
            'first_name' : first_name,
            'use_first_name_alias' : '',
            'last_name': last_name,
            'organization_name' : '',
            'address_purpose': '',
            'city': '',
            'state': '',
            'postal_code': '',
            'country_code': '',
            'limit': '',
            'skip' : '',
            'pretty' : '',
            'version' : 2.1
        }
        r = requests.get(NPPES_NPI_Registry_endpoint, params=params)
        results = r.json()
        if results['result_count'] == 1:
            npi = results["results"][0]['number']
    except Exception as e:
        logging.error('get_provider_api error for ' + str(record['cuimc_id'] + ': '  + str(e) ))
    return npi

def updata_npi_in_redcap(api_key, api_endpoint, record):
    if record['provider_name'] != '':
        logging.info('provider name: ' + str(record['provider_name']) + ' for cuimc_id: ' + str(record['cuimc_id']))
        first_name, last_name = parse_names(record['provider_name'])
        logging.info('provider first name: ' + str(first_name) + ' for cuimc_id: ' + str(record['cuimc_id']))
        logging.info('provider last name: ' + str(last_name) + ' for cuimc_id: ' + str(record['cuimc_id']))
        npi = get_provider_api(first_name, last_name)
        if npi != 0:
            try:
                record['provider_npi'] = str(npi)
                del record['redcap_repeat_instrument']
                del record['redcap_repeat_instance']
                del record['provider_name']
                data = {
                    'token': api_key,
                    'content': 'record',
                    'action': 'import',
                    'format': 'json',
                    'type': 'flat',
                    'overwriteBehavior': 'normal',
                    'forceAutoNumber': 'false',
                    'data': json.dumps([record]),
                    'returnContent': 'count',
                    'returnFormat': 'json'
                }
                r = requests.post(api_endpoint,data=data)
                # logging.info('HTTP Status: ' + str(r.status_code))
            except Exception as e:
                logging.error('updata_npi_in_redcap error for ' + str(record['cuimc_id']) + ': ' + str(e) )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, required=True, help="file to write log",)    
    parser.add_argument('--token', type=str, required=True, help='json file with api tokens')    
    args = parser.parse_args()
    log_file = args.log
    token_file = args.token
    

    logging.basicConfig(filename=log_file, level=logging.INFO)
    # logging.basicConfig(level=logging.INFO)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    logging.info("Current Time =" +  dt_string)
    api_key_local, _, cu_local_endpoint, _ = read_api_config(token_file)
    records = export_data_from_redcap(api_key_local, cu_local_endpoint)
    logging.info("Update NPI...")
    for record in records:
        updata_npi_in_redcap(api_key_local, cu_local_endpoint, record)



