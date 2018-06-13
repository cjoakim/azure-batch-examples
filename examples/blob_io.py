from __future__ import print_function
import argparse
import csv
import datetime
import json
import os
import sys
import time

from azure.storage.blob import BlockBlobService

# python blob_io.py --func list_blobs --cname batchcsv
# python blob_io.py --func upload_state_csv
# python blob_io.py --func download --patterns <comma-separated-patterns>
# python blob_io.py --func download --patterns log-info-states,results-info-states


def create_blob_service():
    account_name = os.environ['AZURE_STORAGE_ACCOUNT']
    account_key  = os.environ['AZURE_STORAGE_KEY']
    return BlockBlobService(account_name=account_name, account_key=account_key)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--func', required=True, help='The logical function to execute')
    parser.add_argument('--cname', required=False, help='The Blob Storage Container name')
    parser.add_argument('--patterns', required=False, help='The logical function to execute')
    args = parser.parse_args()

    blob_svc = create_blob_service()

    if args.func == 'list_blobs':
        print('listing blobs in container {}:'.format(args.cname))
        generator = blob_svc.list_blobs(args.cname)
        blob_list = list()
        for blob in generator:
            print(blob.name)

    elif args.func == 'upload_state_csv':
        states = 'CT,FL,GA,MD,NC,SC,VA'.lower().split(',')
        for st in states:
            print(st)


    elif args.func == 'download':
        patterns = args.patterns.split(',')
        for pattern in patterns:
            print(pattern)
