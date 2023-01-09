import requests
import json
from datetime import datetime
import pandas as pd
import logging
import argparse

import sqlalchemy
import urllib
import configparser
import pyodbc

def read_api_config(config_file):
    logging.info("Reading api tokens and endpoint url...")
    api_token_file = config_file
    with open(api_token_file,'r') as f:
        api_conf = json.load(f)

    api_key_local = api_conf['api_key_local'] # API token for local
    api_key_r4 = api_conf['api_key_r4'] # API token for R4.
    cu_local_endpoint = api_conf['local_endpoint'] # local api endpoint
    r4_api_endpoint = api_conf['r4_api_endpoint'] # R4 api endpoint
    return api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint

def read_sql_connection(configFile='/projects/phi/cl3720/db.conf', database = 'ohdsi_cumc_2022q3r1'):
    logging.info("Reading sql configuration...")
    config = configparser.ConfigParser()
    config.read('/projects/phi/cl3720/db.conf')
        # self.config.sections()
    server = config['ELILEX']['server']
    username = config['ELILEX']['username']
    password = config['ELILEX']['password']
    cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    params = 'Driver={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
    db_params = urllib.parse.quote_plus(params)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))
    cursor = cnxn.cursor()
    return engine, cursor, cnxn

def get_local_mrn(api_key_local,cu_local_endpoint):
    logging.info('Export records...')
    data = {
    'token': api_key_local,
    'content': 'record',
    'action': 'export',
    'format': 'json',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[0]': 'cuimc_id',
    'fields[1]': 'mrn',
    'fields[2]': 'cuimc_empi',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}
    flag = 1
    while(flag > 0 and flag < 5):
        r = requests.post(cu_local_endpoint,data=data)
        if r.status_code == 200:
            logging.info('HTTP Status: ' + str(r.status_code))
            records = r.json()
            flag = 0
        else:
            logging.error('Error occured in exporting data from ' + cu_local_endpoint)
            logging.error('HTTP Status: ' + str(r.status_code))
            logging.error(r.content)
            flag = flag + 1
    return records

def convert_to_empi(records,cnxn):
    '''
    input has 3 keys: cuimc_id, mrn, cuimc_empi
    '''
    logging.info('Convert to EMPI...')
    for record in records:
        if record['cuimc_empi'] != '':
            logging.error("EMPI field is not empty for CUIMC ID: " +  record['cuimc_id'])
            continue
        mrn = record['mrn']
        if mrn == '':
            logging.error("MRN empty for CUIMC ID: " +  record['cuimc_id'])
            continue
        sql = '''
                SELECT DISTINCT M.EMPI
                FROM [mappings].[patient_mappings] M 
                where M.EMPI IN ({l}) or (M.LOCAL_PT_ID IN ({l}) AND (M.FACILITY_CODE = 'P' OR M.FACILITY_CODE = 'UI'))
            '''.format(l = "'" + str(mrn) + "'" )
        mapping_df = pd.read_sql(sql,cnxn)
        if len(mapping_df['EMPI'].tolist()) == 1:
            # unique mapping
            record['cuimc_empi'] = str(mapping_df['EMPI'].tolist()[0])
            logging.info("EMPI converted for CUIMC ID: " +  record['cuimc_id'])

        else:
            logging.error("EMPI not unique for CUIMC ID: " +  record['cuimc_id'])
            logging.error(mrn)
            logging.error(mapping_df)

    upload_data = json.dumps(records)
    data = {
        'token': api_key_local,
        'content': 'record',
        'action': 'import',
        'format': 'json',
        'type': 'flat',
        'overwriteBehavior': 'normal', # to avoid some edge cases
        'forceAutoNumber': 'false',
        'data': upload_data,
        'returnContent': 'count',
        'dateFormat': 'MDY',
        'returnFormat': 'json'
    }

    return data

def execute_import(data, cu_local_endpoint, flag = 1, max_try = 5):
    logging.info('Execute import...')
    while(flag > 0 and flag < max_try):
        r = requests.post(cu_local_endpoint,data=data)
        if r.status_code == 200:
            logging.info('HTTP Status: ' + str(r.status_code))
            flag = 0
        else:
            logging.error('Error occured in importing data to ' + cu_local_endpoint)
            logging.error(r.content)
            flag = flag + 1


if __name__ == "__main__":
    # log_file = '/phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/empi_convert.log'
    # token_file = '/phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/test/test_api_tokens.json'
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

    api_key_local, _, cu_local_endpoint, _ = read_api_config(config_file = token_file)
    print(api_key_local)
    engine, cursor, cnxn = read_sql_connection()
    records = get_local_mrn(api_key_local, cu_local_endpoint)
    upload_data = convert_to_empi(records,cnxn)
    execute_import(upload_data,cu_local_endpoint)    
