

# main function
import shutil
import sys
import csv
import os
from typing import Dict
import pandas as pd
from validate_historical_load_structure import fetch_api_keys
from dotenv import load_dotenv

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


def snake_to_pascal(snake_str, api_keys: Dict[str, str]):
    components = snake_str.lower().split('_')
    pascalText = ''.join(x.title() for x in components)
    _pascalTextUpper = pascalText.upper()
    if _pascalTextUpper in api_keys:
        pascalText = api_keys.get(_pascalTextUpper)
    return pascalText


def build_api_endpoint(service: str, api_endpoint: str) -> str:
    # use .env variable BASE_URL & SUFFIX
    base_url = os.environ.get('BASE_URL')
    suffix = os.environ.get('SUFFIX')
    endpoint = f"{base_url}{service}/{api_endpoint}{suffix}"
    return endpoint


def split_csv_file(api_service: str, api: str, input_file: str, max_file_size_mb: int, output_prefix: str):
    try:
        df = pd.read_csv(input_file, encoding='mac_roman',
                         low_memory=False, dtype=str)
        api_endpoint = build_api_endpoint(api_service, api)
        api_keys = fetch_api_keys(api=api, api_endpoint=api_endpoint)

        # Convert column names to PascalCase
        df.columns = [snake_to_pascal(col, api_keys) for col in df.columns]
        df.to_csv(input_file, index=False)

        max_file_size_bytes = max_file_size_mb * 1024 * 1024
        # Adjust chunk size if necessary
        df_iter = pd.read_csv(input_file, chunksize=10000,
                              encoding='mac_roman', low_memory=False, dtype=str)

        # # Convert column names to PascalCase
        # df_iter.columns = [snake_to_pascal(col) for col in df_iter.columns]

        # # Save the DataFrame back to a CSV file
        # df_iter.to_csv(input_file, index=False)

        file_index = 0
        current_file_size = 0
        chunk_list = []

        output_dir = f"./csv_output/{api}"
        # if dir does not exist, create it
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for chunk in df_iter:
            chunk_size = chunk.memory_usage(deep=True).sum()

            if current_file_size + chunk_size > max_file_size_bytes and chunk_list:
                output_file = f"{output_dir}/{
                    output_prefix}_{file_index}.csv"
                pd.concat(chunk_list).to_csv(output_file, index=False)
                print(f"Written {output_file} with size {
                    current_file_size / (1024 * 1024):.2f} MB")
                chunk_list = []
                file_index += 1
                current_file_size = 0

            chunk_list.append(chunk)
            current_file_size += chunk_size

        if chunk_list:
            output_file = f"{output_dir}/{
                output_prefix}_{file_index}.csv"
            pd.concat(chunk_list).to_csv(output_file, index=False)
            print(f"Written {output_file} with size {
                current_file_size / (1024 * 1024):.2f} MB")
    except Exception as e:
        print(e)


def process_historical_data(api_service: str, api_endpoint: str, historical_data_load_file_name: str):
    # historical_data_load_file_name = db_view_name + ".csv"
    print(f"Check if there is a historical data load file with name: {
          historical_data_load_file_name}")
    # check if historical data load file exists
    historical_data_load_file_path = "./csv_data/" + historical_data_load_file_name
    if not os.path.exists(historical_data_load_file_path):
        print(f"Historical data load file with name: {
            historical_data_load_file_path} does not exist")
        return
    print(f"Historical data load file with name: {
        historical_data_load_file_path} exists")

    # check file size
    file_size = os.path.getsize(historical_data_load_file_path)
    if file_size == 0:
        print(f"Historical data load file with name: {
            historical_data_load_file_path} is empty")
        return

    # max_file_size_mb = int(
    #     input("Enter desirable file size: ")) or 200
    max_file_size_mb = 200

    output_file_prefix = api_endpoint+"_chunk"
    split_csv_file(api_service, api_endpoint, historical_data_load_file_path,
                   max_file_size_mb, output_file_prefix)

    # # load the csv file
    # with open(historical_data_load_file_path, 'r', encoding='mac_roman', newline='') as f_historical_data_load_file:
    #     # use python csv module to read data
    #     reader = csv.reader(f_historical_data_load_file)
    #     # read header row
    #     header = next(reader)
    #     # print(header)
    #     # read data rows
    #     data = list(reader)
    #     num_rows = len(data)
    #     print(f"Found {num_rows} rows in {historical_data_load_file_name}")

    # chunk_size = int(input("Enter desirable number of rows in each file: "))


def process_ifsbronze():
    #    try:
    input_file = "./csv_data/IFS_BronzeAPIControl_20240509.csv"
    if input_file == "":
        print("No input file specified")
        exit(1)
    row_count = 0
    processed_count = 0
    out_folders = []
    # open csv file and read data
    with open(input_file, 'r') as f_input_file:
        # use python csv module to read data
        reader = csv.reader(f_input_file)
        # read header row
        header = next(reader)
        # print(header)
        # read data rows
        data = list(reader)
        for row in data:
            row_count += 1
            api_endpoint = row[7]
            csv_input_file_name = f"{api_endpoint}.csv"
            data_extracted = row[14]
            processed_flag = row[15]
            view_name = row[18]
            # check if View Name has a value
            if data_extracted == 'Y' and csv_input_file_name != "" and processed_flag == 'N':
                process_historical_data(api_endpoint, csv_input_file_name)
                processed_count += 1
                out_folders.append(api_endpoint)
                continue
        print(f"Total number of rows: {
            row_count}. Number of processed rows:{processed_count}")

        # # zip up all the out_folders available under csv_output and save it to a zip file. Name the zip file with current date and timestamp
        # import datetime
        # current_date = datetime.datetime.now().strftime("%Y%m%d")
        # current_time = datetime.datetime.now().strftime("%H%M%S")
        # zip_file_name = f"csv_output_{current_date}_{current_time}.zip"
        # directory_to_zip = "./csv_output"

        # # Create a zip archive
        # shutil.make_archive(zip_file_name.replace(
        #     '.zip', ''), 'zip', directory_to_zip)

    # check if input file
    # except FileNotFoundError:
    #     print(f"File {input_file} not found")


def process_one():
    api_endpoint = "Reference_LedgerTransactionSu"
    api_endpoint = "CustomerOrderJoinSet"
    api_endpoint = "Reference_LedgerTransactionSu"

    api_service = "PartManufacturersHandling.svc"
    api_endpoint = "PartManufacturers"

    api_service = input("Enter the API service: ") or api_service
    api_endpoint = input("Enter the API endpoint: ") or api_endpoint
    csv_input_file_name = f"{api_endpoint}.csv"
    process_historical_data(api_service, api_endpoint, csv_input_file_name)


if __name__ == "__main__":
    process_one()
