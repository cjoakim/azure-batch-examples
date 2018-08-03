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
import io
import json
import os
import string
import sys
import time
import traceback
import uuid

import azure.storage.blob as azureblob

import pandas as pd


VERSION='20180803-1101'


class BlobProcess(object):

    def __init__(self, args):
        self.args = args
        self.log_obj = self.logging_object(args)
        self.add_log_event('__init__')
        self.blob_client = self.create_blob_client()

    def calculate_state_center(self):
        try:
            csv_data = self.read_blob(args.input_container, args.input_blob)

            # use pandas to determine the center of the state
            self.add_log_event('loading csv into dataframe')
            df = pd.read_csv(io.StringIO(csv_data), delimiter=',')
            self.add_log_event('calculating center')
            mean_lat = df["latitude"].mean()
            mean_lng = df["longitude"].mean()
            results_csv_line = '{},{},{}'.format(args.input_blob, mean_lat, mean_lng)

            print('result csv: {}'.format(results_csv_line))
            self.write_blob(args.output_container, args.output_blob, results_csv_line)
            self.add_log_event('processing completed, writing final remote log')
        except:
            self.add_log_event('exception')
            print(sys.exc_info())
            traceback.print_exc()
        finally:
            jstr = json.dumps(self.log_obj, sort_keys=True, indent=2)
            blobname = '{}.json'.format(self.task_id())
            self.write_blob(args.logging_container, blobname, jstr)  
            print(jstr)

    def create_blob_client(self):
        self.add_log_event('creating blob client')
        acct = os.environ["AZURE_STORAGE_ACCOUNT"]
        key  = os.environ["AZURE_STORAGE_KEY"]
        print('create_blob_client: {} {}'.format(acct, key))
        bbs = azureblob.BlockBlobService(account_name=acct, account_key=key)
        self.add_log_event('created blob client')
        return bbs

    def logging_object(self, args):
        log_obj, arg_obj, env_obj = dict(), dict(), dict()
        log_obj['task_id'] = self.task_id()
        log_obj['version'] = VERSION
        log_obj['utc'] = str(arrow.utcnow())
        log_obj['pk'] = str(uuid.uuid1())
        log_obj['events'] = list()
        log_obj['args'] = arg_obj
        log_obj['environment'] = env_obj

        for arg in vars(args):
             arg_obj[arg] = getattr(args, arg)

        for name in sorted(os.environ.keys()):
            env_obj[name] = str(os.environ[name])
        return log_obj

    def add_log_event(self, msg):
        evt = '{}|{}'.format(str(arrow.utcnow()), msg)
        self.log_obj['events'].append(evt)

    def task_id(self):
        # TODO - use AZ BATCH env vars
        return '1'
        
    def read_blob(self, container, blobname):
        msg = 'reading blob {} from container {}'.format(blobname, container)
        self.add_log_event(msg)
        print(msg)
        blob = self.blob_client.get_blob_to_text(container, blobname, 'utf-8')
        return blob.content

    def write_blob(self, container, blobname, content):
        msg = 'writing blob {} to container {}'.format(blobname, container)
        self.add_log_event(msg)
        print(msg)
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
