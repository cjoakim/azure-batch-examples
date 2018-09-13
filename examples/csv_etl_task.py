from __future__ import print_function
import argparse
import collections
import csv
import json
import os
import string
import time
import zipfile

import azure.storage.blob as azureblob

import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors

# Azure Batch Task which will be executed on the Azure Batch nodes.
# It parses the given csv file and inserts the data into Azure CosmosDB.
# Chris Joakim, Microsoft, 2018/09/13

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

def create_docdb_client(args):
    return document_client.DocumentClient(args.docdbhost, {'masterKey': args.docdbkey})

def write_log_data(blob_client, container, args, log_data):
    try:
        # see https://docs.microsoft.com/en-us/azure/batch/batch-compute-node-environment-variables
        job_id  = str(os.environ.get('AZ_BATCH_JOB_ID'))
        task_id = str(os.environ.get('AZ_BATCH_TASK_ID'))
        output_file = '{}-{}-log_data.json'.format(job_id, task_id)
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
    parser.add_argument('--docdbhost', required=True, help='CosmosDB host, AZURE_COSMOSDB_DOCDB_URI')
    parser.add_argument('--docdbkey', required=True, help='CosmosDB key, AZURE_COSMOSDB_DOCDB_KEY')
    parser.add_argument('--dev', required=True, help='Specify True if local development on macOS/Windows')
    args = parser.parse_args()
    epoch = int(time.time())

    print('args.filepath:  {}'.format(args.filepath))
    print('args.storageaccount: {}'.format(args.storageaccount))
    print('args.storagecontainer: {}'.format(args.storagecontainer))
    print('args.sastoken:  {}'.format(args.sastoken))
    print('args.idx:       {}'.format(str(args.idx)))
    print('args.docdbhost: {}'.format(args.docdbhost))
    print('args.docdbkey:  {}'.format(args.docdbkey))
    print('args.dev:       {}'.format(args.dev))
    print('is_dev_env:     {}'.format(is_dev_env(args)))
    print('is_azure_env:   {}'.format(is_azure_env(args)))
    print('epoch:          {}'.format(epoch))

    # Create the blob client using the container's SAS token, and upload the unzipped csv file(s) to it.
    if is_azure_env(args):
        log_data = dict()
        app_events = list()
        log_data['epoch'] = epoch
        log_data['app_events'] = app_events
        log_data['storageaccount'] = args.storageaccount
        log_data['storagecontainer'] = args.storagecontainer
        log_data['sastoken'] = args.sastoken
        log_data['docdbhost'] = args.docdbhost
        log_data['docdbkey'] = args.docdbkey
        log_data['filepath'] = args.filepath
        log_data['dev'] = args.dev

        docdb_client = create_docdb_client(args)
        input_file = os.path.realpath(args.filepath)
        db_link    = 'dbs/dev'
        coll_link  = db_link + '/colls/zipdata'

        print('input_file: {}'.format(input_file))
        log_data['input_file'] = input_file
        log_data['coll_link']  = coll_link

        with open(input_file, 'rt') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            header = None  # id,postal_cd,country_cd,city_name,state_abbrv,latitude,longitude
            for idx, row in enumerate(reader):
                if idx < 1:
                    header = row
                else:
                    data = dict()
                    for fidx, field in enumerate(header):
                        data[field] = row[fidx]  # add each field of the csv to the data dict

                    data['pk'] = data['city_name']  # use city as the CosmosDB partition key
                    data['seq'] = data['id']        # unset the 'id' from the csv, CosmosDB will populate it
                    del data['id']

                    # Add GPS info in GeoJSON format
                    location, lat, lng = dict(), float(data['latitude']), float(data['longitude'])
                    coordinates = [ lng, lat ]
                    location['type'] = 'Point'
                    location['coordinates'] = coordinates
                    data['location'] = location

                    doc = docdb_client.CreateDocument(coll_link, data)
                    print(json.dumps(doc, sort_keys=False, indent=2))

        blob_client = azureblob.BlockBlobService(
            account_name=args.storageaccount,
            sas_token=args.sastoken)
        
        write_log_data(blob_client, args.storagecontainer, args, log_data)
    else:
        print('dev mode; no result blob processing')
