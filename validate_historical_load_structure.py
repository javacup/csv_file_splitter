from dotenv import load_dotenv
import csv
import json
from typing import Dict
import pandas as pd
import requests
from fetch_token import get_access_token
import os
MAX_RETRIES = 1

# Load environment variables from .env file
load_dotenv()

# define token url client id and secret
token_url = os.getenv('TOKEN_URL')

client_id = os.getenv(
    'CLIENT_ID')
client_secret = os.getenv(
    'CLIENT_SECRET')
jwks_url = os.getenv(
    'JWKS_URL')
audience = os.getenv(
    'AUDIENCE')
issuer = os.getenv(
    'ISSUER')
suffix = os.getenv(
    'SUFFIX')


def process_api(endpoint, retry):

    # import the get_access_token function from fetch_token.py
    access_token = get_access_token()  # get the access token

    # add headers to the request
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    # process the api endpoint
    response = requests.get(endpoint, headers=headers)
    if response.status_code == 200:
        return response.json()
    if response.status_code == 401:
        print('Unauthorized.. trying again')
        # retry recursive call to process_api function only once
        if retry > MAX_RETRIES:
            print('Unauthorized.. MAX_RETRIES reached')
            return None
        process_api(endpoint, ++retry)
    else:
        return None

# Function to fetch all keys from the dictionary


def fetch_keys(data, parent_key=''):
    keys = set()
    if isinstance(data, dict):
        for k, v in data.items():
            keys.add(k)
            keys.update(fetch_keys(v))
    elif isinstance(data, list):
        for item in data:
            keys.update(fetch_keys(item))
    return keys

# load historical load csv file


def build_api_endpoint(service: str, api: str) -> str:
    # use .env variable BASE_URL & SUFFIX
    base_url = os.environ.get('BASE_URL')
    suffix = os.environ.get('SUFFIX')
    endpoint = f"{base_url}{service}/{api}{suffix}"
    return endpoint


def fetch_api_keys(api: str, api_endpoint: str) -> Dict[str, str]:
    try:
        json_body_path = f"./json_output/{
            api}"
        json_body_file = f"{json_body_path}/{api}_body.json"
        json_body = None
        # check if json_body_file exists
        if os.path.exists(json_body_file):
            with open(json_body_file, 'r') as f:
                json_body = json.load(f)
        if json_body == None:
            print(f'Processing endpoint: {api_endpoint}')
            # process the api endpoint
            data = process_api(endpoint=api_endpoint, retry=0)
            response = data['response']
            body = response['body']
            json_body = json.loads(body)

            # if json_body_path does not exist create it
            if not os.path.exists(json_body_path):
                os.makedirs(json_body_path)

            # write the json_body to a file named after the api_endpoint
            with open(json_body_file, 'w') as f:
                json.dump(json_body, f)

        # Get all keys
        all_keys = fetch_keys(json_body)

        # sort all_keys
        all_keys = sorted(all_keys)

        fnd_keys = ['keyref', 'luname', 'value',
                    '@odata.context', '@odata.etag', 'Cf_C_Objversion']
        rest_api_attributes = []
        # Print all keys
        for key in all_keys:
            if key in fnd_keys:
                continue
            rest_api_attributes.append(key)
        rest_api_attributes = {key.upper(): key for key in rest_api_attributes}
        return rest_api_attributes
    except Exception as e:
        print(e)


def process_api_endpoint(api: str, api_endpoint: str):
    # historical data load
    historical_data_file = f"./csv_output/{
        api}/{api}_chunk_0.csv"
    # json_body_path = f"./json_output/{
    #     api_endpoint}"
    # json_body_file = f"{json_body_path}/{api_endpoint}_body.json"
    # json_body = None
    # load csv file and fetch the header
    try:
        df = pd.read_csv(historical_data_file,
                         encoding='mac_roman', low_memory=False, dtype=str)
    except Exception as e:
        print(e)

    # check if json_body_file exists
    # if os.path.exists(json_body_file):
    #     with open(json_body_file, 'r') as f:
    #         json_body = json.load(f)

    # if json_body == None:
    #     print(f'Processing endpoint: {endpoint}')
    #     # process the api endpoint
    #     data = process_api(endpoint=endpoint, retry=0)
    #     response = data['response']
    #     body = response['body']
    #     json_body = json.loads(body)

    #     # if json_body_path does not exist create it
    #     if not os.path.exists(json_body_path):
    #         os.makedirs(json_body_path)

    #     # write the json_body to a file named after the api_endpoint
    #     with open(json_body_file, 'w') as f:
    #         json.dump(json_body, f)

    # # Get all keys
    # all_keys = fetch_keys(json_body)

    # # sort all_keys
    # all_keys = sorted(all_keys)

    # fnd_keys = ['keyref', 'luname', 'value',
    #             '@odata.context', '@odata.etag', 'Cf_C_Objversion']
    # rest_api_attributes = []
    # # Print all keys
    # for key in all_keys:
    #     if key in fnd_keys:
    #         continue
    #     rest_api_attributes.append(key)

    # from the panda dataframe, fetch the columns
    columns = df.columns

    # convert this to a set
    historical_data_columns = {col.upper(): col for col in columns}

    incremental_data_columns = fetch_api_keys(
        api=api, api_endpoint=api_endpoint)
    # {
    #     key.upper(): key for key in rest_api_attributes}

    # print(historical_data_columns)
    # incremental_data_columns = set([key.upper() for key in rest_api_attributes])
    print(f"Total REST API Attribute count: {len(incremental_data_columns)} \nTotal Select Query Column count:{
          len(historical_data_columns)} \n\n")

    matching_list = []
    non_matching_list = []

    # # loop through incremental data columns and check if they exist in historical data columns
    # for col in incremental_data_columns:
    #     if col not in historical_data_columns.keys():
    #         # print(f'{col} does not exist in historical data')
    #         non_matching_list.append(col)
    #     else:
    #         matching_list.append(col)
    # for key, value in historical_data_columns.items():
    #     if key in incremental_data_columns:
    #         matching_list.append(value)
    #     else:
    #         non_matching_list.append(value)
    for key, value in incremental_data_columns.items():
        if key in historical_data_columns.keys():
            matching_list.append(value)
        else:
            non_matching_list.append(value)

    matching_list = list(set(matching_list))
    non_matching_list = list(set(non_matching_list))

    print(f'{historical_data_file}: \nMatching Attribute count: {
        len(matching_list)} \nNon-matching Attribute count: {len(non_matching_list)}\n\n')

    # raise an exception or handle the error appropriately
    if len(non_matching_list) > 0:
        print(
            f'Non-matching Attributes: {non_matching_list} \n in {historical_data_file}')


def load_historical_load_csv_file():
    input_file = "./csv_data/IFS_BronzeAPIControl_20240509.csv"
    row_count = 0
    # open csv file and read data
    with open(input_file, 'r') as f_input_file:
        # use python csv module to read data
        reader = csv.reader(f_input_file)
        # read header row
        header = next(reader)
        data = list(reader)
        for row in data:
            row_count += 1
            service = row[5]
            api_endpoint = row[7]
            endpoint = build_api_endpoint(
                service=service, api_endpoint=api_endpoint)
            process_api_endpoint(api_endpoint, endpoint)


def main():
    try:
        # load_historical_load_csv_file()
        # get input for service and api endpoint
        service = input(
            "Enter service name: ") or 'PurchaseOrderLinesHandling.svc'
        api = input("Enter api : ") or 'PurchaseOrderLineSet'
        api_endpoint = build_api_endpoint(
            service=service, api=api)
        process_api_endpoint(api, api_endpoint)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
