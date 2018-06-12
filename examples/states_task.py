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

def write_log_data(blob_client, container, args, log_data):
    try:
        output_file = 'states_task_log_data_{}.json'.format(str(args.idx))
        output_file_path = os.path.realpath(output_file)     
        log_json = json.dumps(log_data, sort_keys=True, indent=2)
        with open(output_file, 'w') as f:
            f.write(log_json)
        blob_client.create_blob_from_path(
            container,
            output_file,
            output_file_path)
    except KeyError:
        app_events.append('ERROR in write_log_data')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--filepath', required=True, help='The path to the zip file to process')
    parser.add_argument('--storageaccount', required=True, help='The name the Azure Storage account for results.')
    parser.add_argument('--storagecontainer', required=True, help='The Azure Blob storage container for results.')
    parser.add_argument('--sastoken', required=True, help='The SAS token providing write access to the Storage container.')
    parser.add_argument('--idx', required=True, help='The index number of the file within the job')
    args = parser.parse_args()
    epoch = int(time.time())

    print('args.filepath:  {}'.format(args.filepath))
    print('args.storageaccount: {}'.format(args.storageaccount))
    print('args.storagecontainer: {}'.format(args.storagecontainer))
    print('args.sastoken:  {}'.format(args.sastoken))
    print('args.idx:       {}'.format(str(args.idx)))
    print('epoch:          {}'.format(epoch))

    # Create the blob client using the container's SAS token, and upload the unzipped csv file(s) to it.
    log_data = dict()
    app_events = list()
    log_data['args'] = args
    log_data['epoch'] = epoch
    log_data['app_events'] = app_events
    log_data['storageaccount'] = args.storageaccount
    log_data['storagecontainer'] = args.storagecontainer
    log_data['sastoken'] = args.sastoken
    log_data['filepath'] = args.filepath

    input_file = os.path.realpath(args.filepath)

    print('input_file: {}'.format(input_file))
    log_data['input_file'] = input_file
    log_data['coll_link']  = coll_link

    # with open(input_file, 'rt') as csvfile:
    #     reader = csv.reader(csvfile, delimiter=',')
    #     header = None  # id,postal_cd,country_cd,city_name,state_abbrv,latitude,longitude
    #     for idx, row in enumerate(reader):
    #         if idx < 1:
    #             header = row
    #         else:
    #             data = dict()
    #             for fidx, field in enumerate(header):
    #                 data[field] = row[fidx]
    #             data['pkey'] = data['city_name']
    #             data['seq'] = data['id']
    #             del data['id']
    #             doc = docdb_client.CreateDocument(coll_link, data)
    #             print(doc)

    log_data['app_events'] = app_events
    
    blob_client = azureblob.BlockBlobService(
        account_name=args.storageaccount,
        sas_token=args.sastoken)
        
    write_log_data(blob_client, args.storagecontainer, args, log_data)
