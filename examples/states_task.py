from __future__ import print_function
import argparse
import collections
import csv
import json
import os
import string
import sys
import time
import zipfile

import pandas as pd

import azure.storage.blob as azureblob


# Azure Batch Task which will be executed on the Azure Batch nodes.
# Reads the specified US_state csv blob files and calculates the mean latitude and longitude of the state. 
# Chris Joakim, Microsoft, 2018/06/12

def is_dev_env(args):
    if ('' + args.dev).lower() == 'true':
        return True
    else:
        return False

def is_azure_env(args):
    if is_dev_env(args):
        return False
    else:
        return True

def describe_df(df):
    print('=== type(df)')
    print(type(df))  # <class 'pandas.core.frame.DataFrame'>
    print('=== df.columns')
    print(df.columns)
    print('=== df.head()')
    print(df.head())
    print('=== df.tail(1)')
    print(df.tail(1))
    print('=== df.index')
    print(df.index)
    print('=== df.dtypes')
    print(df.dtypes)
    print('=== df.describe()')
    print(df.describe())

def task_logic(args_file):
    print('task_logic; args_file:  {}'.format(args_file))
    input_file = os.path.realpath(args_file)
    print('task_logic; input_file: {}'.format(input_file))
    df = pd.read_csv(input_file, delimiter=',')
    describe_df(df)
    mean_lat = df["latitude"].mean()
    mean_lng = df["longitude"].mean()
    return '{},{},{}'.format(args_file, mean_lat, mean_lng)

def write_log_data(blob_client, container, args, log_data):
    output_file = 'states_task_log_data_{}.json'.format(str(args.idx))
    output_file_path = os.path.realpath(output_file)     
    log_json = json.dumps(log_data, sort_keys=True, indent=2)
    with open(output_file, 'w') as f:
        f.write(log_json)
    blob_client.create_blob_from_path(
        container,
        output_file,
        output_file_path)


if __name__ == '__main__':
    # example command line:
    # python states_task.py --filepath data/postal_codes_ct.csv --storageaccount NA --storagecontainer NA --sastoken NA --idx 0 --dev true
    # python $AZ_BATCH_NODE_SHARED_DIR/states_task.py --filepath postal_codes_ct.csv --storageaccount cjoakimstdstorage --storagecontainer batchcsv --sastoken "se=2018-06-12T23%3A26%3A08Z&sp=w&sv=2017-04-17&sr=c&sig=ryekDDb0WEwVlqC2vdmQ8aUFSDNbh3zqaup%2BmZJ%2B7po%3D" --idx 0']

    parser = argparse.ArgumentParser()
    parser.add_argument('--filepath', required=True, help='The path to the csv file to process')
    parser.add_argument('--storageaccount', required=True, help='The name the Azure Storage account for results.')
    parser.add_argument('--storagecontainer', required=True, help='The Azure Blob storage container for results.')
    parser.add_argument('--sastoken', required=True, help='The SAS token providing write access to the Storage container.')
    parser.add_argument('--idx', required=True, help='The index number of the file within the job')
    parser.add_argument('--dev', required=True, help="Specify 'true' if local development on macOS/Windows", default='false')
    args = parser.parse_args()
    epoch = int(time.time())

    # Create the blob client using the container's SAS token, and upload the unzipped csv file(s) to it.
    log_data = dict()
    app_events = list()
    log_data['args.filepath'] = args.filepath
    log_data['args.storageaccount'] = args.storageaccount
    log_data['args.storagecontainer'] = args.storagecontainer
    log_data['args.sastoken'] = args.sastoken
    log_data['args.idx'] = args.idx
    log_data['args.dev'] = args.dev
    log_data['epoch'] = epoch
    log_data['app_events'] = app_events
    print(json.dumps(log_data, sort_keys=False, indent=2))

    if is_azure_env(args):
        print('azure environment')
        csv_line = 'disabled,for,now'  # task_logic(args.filepath)
        print('csv_line: {}'.format(csv_line))

        blob_client = azureblob.BlockBlobService(
            account_name=args.storageaccount,
            sas_token=args.sastoken)
    
        output_file = 'mean_{}'.format(args.filepath)
        # output_file_path = os.path.realpath(output_file)
        # with open(output_file, 'w') as f:
        #     f.write(csv_line)

        blob_client.create_blob_from_text(
            args.storagecontainer,
            output_file,
            csv_line)

        # blob_client.create_blob_from_path(
        #     args.storagecontainer,
        #     output_file,
        #     output_file_path)
    else:
        print('workstation environment')   
        csv_line = task_logic(args.filepath)
        print('csv_line: {}'.format(csv_line))
