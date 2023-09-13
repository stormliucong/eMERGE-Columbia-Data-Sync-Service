import requests
import json

# change api_tokens.json to your own api_tokens.json file

def read_api_config(config_file = '../api_tokens.json'):
    api_token_file = config_file
    with open(api_token_file,'r') as f:
        api_conf = json.load(f)

    api_key_local = api_conf['api_key_local'] # API token for local
    api_key_r4 = api_conf['api_key_r4'] # API token for R4.
    cu_local_endpoint = api_conf['local_endpoint'] # local api endpoint
    r4_api_endpoint = api_conf['r4_api_endpoint'] # R4 api endpoint
    return api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint

def get_api_version(api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint):
    data = {
        'token': api_key_local,
        'content': 'version'
    }
    # change verify to False if you are using self-signed certificate
    r = requests.post(cu_local_endpoint,data=data, verify=False)
    print('HTTP Status: ' + str(r.status_code))
    print('Local redcap version : ' + str(r.content))
    data = {
        'token': api_key_r4,
        'content': 'version'
    }
    # change verify to False if you are using self-signed certificate
    r = requests.post(r4_api_endpoint,data=data, verify=False)
    print('HTTP Status: ' + str(r.status_code))
    print('R4 redcap version : ' + str(r.content))

if __name__ == "__main__":
    api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint = read_api_config()
    get_api_version(api_key_local, api_key_r4, cu_local_endpoint, r4_api_endpoint)


