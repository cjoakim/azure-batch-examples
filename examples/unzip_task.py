from __future__ import print_function
import argparse
import collections
import datetime
import json
import os
import string
import time
import zipfile

import azure.storage.blob as azureblob

# Azure Batch Task which will be executed on the Azure Batch nodes.
# It unzips the specified zip file and stores the extraced csv files
# in Azure Blob Storage.
# Chris Joakim, Microsoft, 2018/09/13

def get_entry_names_list(zf):
    entries = list()
    for info in zf.infolist():
        fn = info.filename
        entries.append(fn)
        cs = info.create_system
        cv = info.create_version
        cb = info.compress_size
        ub = info.file_size
        print('zipfile entry: {} cs: {} cv: {} cb: {} ub: {}'.format(fn, cs, cv, cb, ub))
    return entries

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

def write_log_data(blob_client, container, log_data):
    try:
        output_file = 'unzip_task_log_data.json'
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
    print('Batch Task {} at {}'.format(__file__, datetime.datetime.utcnow()))
    parser = argparse.ArgumentParser()
    parser.add_argument('--filepath',        required=True, help='The path to the zip file to process')
    parser.add_argument('--storageaccount',  required=True, help='The name the Azure Storage account for results.')
    parser.add_argument('--outputcontainer', required=True, help='The Azure Blob storage container for results.')
    parser.add_argument('--outputtoken',     required=True, help='The SAS token providing write access to the Storage container.')
    parser.add_argument('--dev', required=True, help='Specify True if local development on macOS/Windows')
    args = parser.parse_args()
    epoch = int(time.time())

    print('args.filepath: {}'.format(args.filepath))
    print('args.storageaccount: {}'.format(args.storageaccount))
    print('args.outputcontainer: {}'.format(args.outputcontainer))
    print('args.outputtoken: {}'.format(args.outputtoken))
    print('args.dev:      {}'.format(args.dev))
    print('is_dev_env:    {}'.format(is_dev_env(args)))
    print('is_azure_env:  {}'.format(is_azure_env(args)))
    print('epoch:         {}'.format(epoch))

    input_file = os.path.realpath(args.filepath)
    print('input_file: {}'.format(input_file))

    if zipfile.is_zipfile(input_file):
        print('{} IS a zip file'.format(input_file))
        zf = zipfile.ZipFile(input_file)
        entry_names = get_entry_names_list(zf)
        for entry_name in entry_names:
            if is_dev_env(args):
                print('dev mode; reading entry: {}'.format(entry_name))
                try:
                    data = zf.read(entry_name)
                except KeyError:
                    print('ERROR: Did not find entry "{}" in zip file'.format(entry_name))
                else:
                    print(repr(data))
            else:
                print('entry: {}'.format(entry_name))
    else:
        print('{} IS NOT a zip file'.format(input_file))

    # Create the blob client using the container's SAS token, and upload the unzipped csv file(s) to it.
    if is_azure_env(args):
        log_data = dict()
        app_events = list()
        log_data['app_events'] = app_events

        blob_client = azureblob.BlockBlobService(
            account_name=args.storageaccount,
            sas_token=args.outputtoken)

        for entry_name in entry_names:
            app_events.append('processing entry: {}'.format(entry_name))
            try:
                data = zf.read(entry_name)
                output_file = entry_name
                output_file_path = os.path.realpath(output_file)
                with open(output_file, 'w') as f:
                    f.write(data)
                    app_events.append('file written: {} {}'.format(output_file, output_file_path))
                blob_client.create_blob_from_path(
                    args.outputcontainer,
                    output_file,
                    output_file_path)
            except KeyError:
                app_events.append('ERROR: Did not find entry "{}" in zip file'.format(entry_name))

        write_log_data(blob_client, args.outputcontainer, log_data)
    else:
        print('dev mode; no result blob processing')
