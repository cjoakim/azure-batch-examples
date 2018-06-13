from __future__ import print_function
import argparse
import collections
import csv
import json
import os
import os.path
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
    if (str(args.dev)).lower() == 'true':
        return True
    else:
        return False

def is_azure_env(args):
    if is_dev_env(args):
        return False
    else:
        return True


# example command lines:
# local -> python states_task.py --filepath data/postal_codes_ct.csv --storageaccount NA --storagecontainer NA --sastoken NA --idx 0 --dev true
# azure -> python $AZ_BATCH_NODE_SHARED_DIR/states_task.py --filepath postal_codes_ct.csv --storageaccount cjoakimstdstorage --storagecontainer batchcsv --sastoken "se=2018-06-13T17%3A01%3A47Z&sp=w&sv=2017-04-17&sr=c&sig=PcfjDshyJZWmvCPTpJE0Em0VTJHBCsTF0IwJe9t4WDI%3D" --idx 0 --dev false

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--filepath', required=True, help='The path to the csv file to process')
    parser.add_argument('--storageaccount', required=True, help='The name the Azure Storage account for results.')
    parser.add_argument('--storagecontainer', required=True, help='The Azure Blob storage container for results.')
    parser.add_argument('--sastoken', required=True, help='The SAS token providing write access to the Storage container.')
    parser.add_argument('--idx', required=True, help='The index number of the file within the job')
    parser.add_argument('--dev', required=True, help="Specify 'true' if local development on macOS/Windows", default='false')
    args = parser.parse_args()
    epoch = int(time.time())

    # Create and populate a dictionary for logging purposes.
    log_obj = dict()
    log_obj['args.filepath'] = args.filepath
    log_obj['args.storageaccount'] = args.storageaccount
    log_obj['args.storagecontainer'] = args.storagecontainer
    log_obj['args.sastoken'] = args.sastoken
    log_obj['args.idx'] = args.idx
    log_obj['args.dev'] = args.dev
    log_obj['epoch'] = epoch

    # Azure Batch adds some standard well-defined environment variables.
    # see https://docs.microsoft.com/en-us/azure/batch/batch-compute-node-environment-variables
    job_id  = str(os.environ.get('AZ_BATCH_JOB_ID'))
    task_id = str(os.environ.get('AZ_BATCH_TASK_ID'))

    # Capture the environment variables for logging.
    for name in sorted(os.environ.keys()):
        log_key = 'env.{}'.format(name)
        log_obj[log_key] = str(os.environ[name])

    if is_azure_env(args):
        log_obj['env'] = 'azure'
        fq_input_file = os.path.realpath(args.filepath)
        log_obj['fq_input_file'] = fq_input_file

        # Calculate the output blob filenames
        results_csv_filename = 'results-info-{}-{}-{}'.format(job_id, task_id, args.filepath)
        log_json_filename = 'log-info-{}-{}.json'.format(job_id, task_id)
        log_obj['results_csv_filename'] = results_csv_filename
        log_obj['log_json_filename'] = log_json_filename

        # Use Pandas to get the geographical center of the State; the mean of longitude and latitude values.
        df = pd.read_csv(fq_input_file, delimiter=',')
        mean_lat = df["latitude"].mean()
        mean_lng = df["longitude"].mean()
        results_csv_line = '{},{},{},{},{}'.format(job_id, task_id, args.filepath, mean_lat, mean_lng)

        # Create a blob client for writing the output blobs.
        blob_client = azureblob.BlockBlobService(
            account_name=args.storageaccount,
            sas_token=args.sastoken)
    
        # Write the CSV blob with the Pandas-calculated values.

        blob_client.create_blob_from_text(
            args.storagecontainer,
            results_csv_filename,
            results_csv_line)

        # Also write the Log blob in JSON format.

        log_json = json.dumps(log_obj, sort_keys=True, indent=2)
        blob_client.create_blob_from_text(
            args.storagecontainer,
            log_json_filename,
            log_json)
    else:
        # This branch is used to sanity-check this task file locally on your workstation
        # before submitting the job to Azure Batch.
        log_obj['env'] = 'workstation'
        print(json.dumps(log_obj, sort_keys=True, indent=2))


# Example CSV results:
# states_1528902104,task1,postal_codes_ct.csv,41.5558505486,-72.8087721435

# Example log JSON:
# {
#   "args.dev": "false", 
#   "args.filepath": "postal_codes_ct.csv", 
#   "args.idx": "0", 
#   "args.sastoken": "se=2018-06-13T16%3A48%3A56Z&sp=w&sv=2017-04-17&sr=c&sig=7eVvit3/.....", 
#   "args.storageaccount": "cjoakimstdstorage", 
#   "args.storagecontainer": "batchcsv", 
#   "env": "azure", 
#   "env.AZ_BATCH_ACCOUNT_NAME": "cjoakimbatch2", 
#   "env.AZ_BATCH_ACCOUNT_URL": "https://cjoakimbatch2.eastus.batch.azure.com/", 
#   "env.AZ_BATCH_CERTIFICATES_DIR": "/mnt/batch/tasks/workitems/states_1528901334/job-1/task1/certs", 
#   "env.AZ_BATCH_JOB_ID": "states_1528901334", 
#   "env.AZ_BATCH_NODE_ID": "tvm-3657382398_1-20180613t145056z", 
#   "env.AZ_BATCH_NODE_IS_DEDICATED": "true", 
#   "env.AZ_BATCH_NODE_ROOT_DIR": "/mnt/batch/tasks", 
#   "env.AZ_BATCH_NODE_SHARED_DIR": "/mnt/batch/tasks/shared", 
#   "env.AZ_BATCH_NODE_STARTUP_DIR": "/mnt/batch/tasks/startup", 
#   "env.AZ_BATCH_POOL_ID": "statespool_1528901334", 
#   "env.AZ_BATCH_TASK_DIR": "/mnt/batch/tasks/workitems/states_1528901334/job-1/task1", 
#   "env.AZ_BATCH_TASK_ID": "task1", 
#   "env.AZ_BATCH_TASK_USER": "_azbatch", 
#   "env.AZ_BATCH_TASK_USER_IDENTITY": "PoolNonAdmin", 
#   "env.AZ_BATCH_TASK_WORKING_DIR": "/mnt/batch/tasks/workitems/states_1528901334/job-1/task1/wd", 
#   "env.HOME": "/mnt/batch/tasks/workitems/states_1528901334/job-1/task1/wd", 
#   "env.PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/mnt/batch/tasks/shared:/mnt/batch/tasks/workitems/states_1528901334/job-1/task1/wd", 
#   "env.PWD": "/mnt/batch/tasks/workitems/states_1528901334/job-1/task1/wd", 
#   "env.SHLVL": "1", 
#   "env.USER": "_azbatch", 
#   "env._": "/usr/bin/python", 
#   "epoch": 1528901519, 
#   "fq_input_file": "/mnt/batch/tasks/workitems/states_1528901334/job-1/task1/wd/postal_codes_ct.csv"
# }