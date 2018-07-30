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
import traceback
import zipfile

import pandas as pd

import azure.storage.blob as azureblob


# Azure Batch Task which will be executed on the Azure Batch nodes.
# Reads the specified US_state csv blob files and calculates the mean latitude and longitude of the state. 
# Chris Joakim, Microsoft, 2018/07/26

start_epoch = int(time.time())

def is_dryrun(args):
    # "dryrun" mode is intended to mininimally execute the Task in order to verify
    # the parameters and runtime environment.
    return str(args.dryrun).lower() == 'y'

def write_output_blob(client, args, blobname, blobtext):
    client.create_blob_from_text(args.outputcontainer, blobname, blobtext)

def write_logging_blob(client, args, name, blobtext):
    job_id = str(os.environ.get('AZ_BATCH_JOB_ID'))
    task_id = str(os.environ.get('AZ_BATCH_TASK_ID'))
    curr_epoch = int(time.time())
    blobname = '{}-{}-{}-{}-{}.log'.format(job_id, task_id, start_epoch, name, curr_epoch)
    client.create_blob_from_text(args.loggingcontainer, blobname, blobtext)


if __name__ == '__main__':

    # 'python $AZ_BATCH_NODE_SHARED_DIR/{} --filepath {} --storageaccount {} --outputcontainer {} --outputtoken "{}" --loggingcontainer {} --loggingtoken "{}" --idx {} --dryrun {}'

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--filepath',         required=True, help='The path to the csv file to process')
        parser.add_argument('--storageaccount',   required=True, help='The name the Azure Storage account for the output and logging container.')
        parser.add_argument('--outputcontainer',  required=True, help='The Azure Blob storage container for output.')
        parser.add_argument('--outputtoken',      required=True, help='The SAS token providing write access to the output container.')
        parser.add_argument('--loggingcontainer', required=True, help='The Azure Blob storage container for logging.')
        parser.add_argument('--loggingtoken',     required=True, help='The SAS token providing write access to the logging container.')
        parser.add_argument('--idx',              required=True, help='The index number of the file within the job')
        parser.add_argument('--dryrun',           required=False, help='Optionally specify y for dry-run mode with minimal task functionality', default='n')
        args = parser.parse_args()

        output_blob_client, logging_blob_client = None, None

        if is_dryrun(args):
            print('dryrun; not creating blob clients')
        else:
            print('creating output_blob_client...')
            output_blob_client = azureblob.BlockBlobService(
                account_name=args.storageaccount,
                sas_token=args.outputtoken)
            print('creating logging_blob_client...')
            logging_blob_client = azureblob.BlockBlobService(
                account_name=args.storageaccount,
                sas_token=args.loggingtoken)
            write_logging_blob(
                logging_blob_client, args, 'boj', 'start time is: {}'.format(start_epoch))

        # Create and populate a dictionary for logging purposes.
        log_obj = dict()
        log_obj['args.filepath'] = args.filepath
        log_obj['args.storageaccount'] = args.storageaccount
        log_obj['args.outputcontainer'] = args.outputcontainer
        log_obj['args.outputtoken'] = args.outputtoken
        log_obj['args.loggingcontainer'] = args.outputcontainer
        log_obj['args.loggingtoken'] = args.outputtoken
        log_obj['args.idx'] = args.idx
        log_obj['args.dryrun'] = args.dryrun
        log_obj['start_epoch'] = start_epoch

        # Azure Batch adds some standard well-defined environment variables.
        # see https://docs.microsoft.com/en-us/azure/batch/batch-compute-node-environment-variables
        job_id  = str(os.environ.get('AZ_BATCH_JOB_ID'))
        task_id = str(os.environ.get('AZ_BATCH_TASK_ID'))

        # Capture the environment variables for logging.
        for name in sorted(os.environ.keys()):
            log_key = 'env.{}'.format(name)
            log_obj[log_key] = str(os.environ[name])

        env_json = json.dumps(log_obj, sort_keys=True, indent=2)
        print(env_json)

        if is_dryrun(args):
            print('dryrun; not executing task logic.  task complete.')
        else:
            write_logging_blob(
                logging_blob_client, args, 'environment', env_json)

            log_obj['env'] = 'azure'
            fq_input_file = os.path.realpath(args.filepath)
            log_obj['fq_input_file'] = fq_input_file

            # Calculate the output blob filenames
            results_csv_filename = 'results-info-{}-{}-{}'.format(job_id, task_id, args.filepath)
            log_json_filename = 'log-info-{}-{}.json'.format(job_id, task_id)
            log_obj['results_csv_filename'] = results_csv_filename
            log_obj['log_json_filename'] = log_json_filename
            write_logging_blob(
                logging_blob_client, args, 'calc-filenames', env_json)

            # Use Pandas to get the geographical center of the State; the mean of longitude and latitude values.
            df = pd.read_csv(fq_input_file, delimiter=',')
            mean_lat = df["latitude"].mean()
            mean_lng = df["longitude"].mean()
            results_csv_line = '{},{},{},{},{}'.format(job_id, task_id, args.filepath, mean_lat, mean_lng)

            # Write the CSV blob with the Pandas-calculated values.
            write_output_blob(
                output_blob_client, args, results_csv_filename, results_csv_line)

            log_json = json.dumps(log_obj, sort_keys=True, indent=2)
            write_logging_blob(
                logging_blob_client, args, 'eoj', log_json)
    except:
        print(sys.exc_info())
        traceback.print_exc()
