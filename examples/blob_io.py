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
# python blob_io.py --func upload_state_csv --cname batchcsv
# python blob_io.py --func download --cname batchcsv --patterns <comma-separated-patterns>
# python blob_io.py --func download --cname batchcsv --patterns postal_codes,log-info-states,results-info-states


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
            basename = 'postal_codes_{}.csv'.format(st)
            infile = 'data/{}'.format(basename)
            blob_svc.create_blob_from_path(args.cname, basename, infile)
            print('uploaded {} to {}'.format(infile, args.cname))

    elif args.func == 'download':
        print('downloading blobs from container {} ...'.format(args.cname))
        patterns  = args.patterns.split(',')
        generator = blob_svc.list_blobs(args.cname)
        blob_list = list()
        for blob in generator:
            matched = False
            for pattern in patterns:
                if pattern in blob.name:
                    matched = True
            if matched:
                outfile = 'tmp/{}'.format(blob.name)
                blob_svc.get_blob_to_path(args.cname, blob.name, outfile)
                print('downloaded {}'.format(blob.name))
