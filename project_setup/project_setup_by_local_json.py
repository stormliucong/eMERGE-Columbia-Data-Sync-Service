import requests
import json
import argparse # for command line arguments


# read api tokens from json file.
argparser = argparse.ArgumentParser()
argparser.add_argument('--api_token_file', help='path to api token file', required=False)
argparser.add_argument('--dict_json', help='path to dictionary meta file', required=True)

args = argparser.parse_args()
if args.api_token_file:
    api_token_file = args.api_token_file
else:
    api_token_file = '../api_tokens.json'

with open(api_token_file,'r') as f:
    api_conf = json.load(f)
    
with open(args.dict_json,'r') as f:
    new_json = json.load(f)

api_key_local = api_conf['api_key_local'] # API token for local
cu_local_endpoint = api_conf['local_endpoint'] # local api endpoint

# update local data dictionary
data = {
    'token': api_key_local,
    'content': 'metadata',
    'format': 'json',
    'returnFormat': 'json',
    'data': json.dumps(new_json)
}
# change verify to False if you are using self-signed certificate
r = requests.post(cu_local_endpoint,data=data, verify=False)
print('HTTP Status: ' + str(r.status_code))
print('Number of fields: ' + r.content.decode('utf-8'))

# HTTP Status: {"error":"This method cannot be used while the project is in Production status."}
# Move Back to Development status.

# Have to config the repeated instrument manually on the web interface.

