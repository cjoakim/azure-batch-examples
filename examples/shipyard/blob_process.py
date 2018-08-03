"""
Usage:
  python blob_process.py --function state_center --input_container batchin --input_blob postal_codes_ct.csv --output_container batchout --output_blob result_ct.csv --logging_container batchlog
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# Chris Joakim, Microsoft, 2018/08/03

from __future__ import print_function
import argparse
import arrow
import collections
import csv
import datetime
import json
import os
import string
import sys
import time
import traceback
import uuid

import azure.storage.blob as azureblob

VERSION='20180803-1008'


class BlobProcess(object):

    def __init__(self, args):
        self.args = args
        self.blob_client = self.create_blob_client()

    def calculate_state_center(self):
        print('calculate_state_center')
        print(args)
        # "input_container": "batchin",
        # "input_blob": "postal_codes_ct.csv",
        # "output_container": "batchout",
        # "output_blob": "result_ct.csv",
        # "logging_container": "batchlog"

    def create_blob_client(self):
        acct = os.environ["AZURE_STORAGE_ACCOUNT"]
        key  = os.environ["AZURE_STORAGE_KEY"]
        print('create_blob_client: {} {}'.format(acct, key))
        return azureblob.BlockBlobService(account_name=acct, account_key=key)

    def logging_object():
        log_obj, env_obj = dict(), dict()
        log_obj['version'] = VERSION
        log_obj['utc'] = str(arrow.utcnow())
        log_obj['pk'] = str(uuid.uuid1())
        log_obj['events'] = list()
        log_obj['environment'] = env_obj
        for name in sorted(os.environ.keys()):
            env_obj[name] = str(os.environ[name])
        return log_obj

    def read_blob(self, container, blobname):
        blob = self.blob_client.get_blob_to_text(container, blobname, 'utf-8')
        return blob.content

    def write_blob(self, container, blobname, content):
        print('writing blob: {} {} ...'.format(container, blobname))
        self.blob_client.create_blob_from_text(container, blobname, content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--function', required=True, help='The logical function to execute')
    parser.add_argument('--input_container', required=True)
    parser.add_argument('--input_blob', required=True)
    parser.add_argument('--output_container', required=True)
    parser.add_argument('--output_blob', required=True)
    parser.add_argument('--logging_container', required=True)
    args = parser.parse_args()

    if args.function == 'state_center':
        p = BlobProcess(args)
        p.calculate_state_center()

    elif args.function == 'state_info':
        print('function not yet implemented: {}'.format(args.function))

    else:
        print('unknown function: {}'.format(args.function))
