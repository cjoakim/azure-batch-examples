from __future__ import print_function
import argparse
import datetime 
import json
import os
import sys
import time
import traceback

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

sys.path.append('.')
import helpers
from batch_client import BatchClient

# Azure Batch client program which submits a job.
# Chris Joakim, Microsoft, 2018/07/26


class StatesBatchClient(BatchClient):

    def __init__(self, args):
        BatchClient.__init__(self, args)

    def add_tasks(self):
        tasks = list()
        sas_token_cin  = self.get_container_sas_token(self.args.cin)   # input container sas token
        sas_token_clog = self.get_container_sas_token(self.args.clog)  # logging container sas token

        cin_container_sas_token = self.get_container_sas_token(self.args.cin)
        permission = azureblob.models.BlobPermissions(read=True, add=True, create=True, write=True, delete=True)

        for idx, blob in enumerate(self.blob_input_files):
            sas_token = self.blob_client.generate_blob_shared_access_signature(
                self.args.cin,
                blob.name,
                permission=permission,
                expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

            sas_url = self.blob_client.make_blob_url(
                self.args.cin,
                blob.name,
                sas_token=sas_token)

            template = 'python $AZ_BATCH_NODE_SHARED_DIR/{} --filepath {} --storageaccount {} --outputcontainer {} --outputtoken "{}" --loggingcontainer {} --loggingtoken "{}" --idx {} --dryrun {}'
            command  = [
                template.format(
                    self.TASK_FILE,
                    blob.name,
                    self.STORAGE_ACCOUNT_NAME,
                    self.args.cin,
                    sas_token_cin,
                    self.args.clog,
                    sas_token_clog,
                    str(idx),
                    self.args.dryrun
                )
            ]
            print('command: {}'.format(command))
            tasks.append(batch.models.TaskAddParameter(
                'task{}'.format(idx+1),
                helpers.wrap_commands_in_shell('linux', command),
                resource_files=[batchmodels.ResourceFile(file_path=blob.name, blob_source=sas_url)]))

        self.batch_client.task.add_collection(self.JOB_ID, tasks)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pool',      required=True,  help='The name of the Azure Batch Pool')
    parser.add_argument('--job',       required=True,  help='The name of the Azure Batch Job')
    parser.add_argument('--task',      required=True,  help='The name the Task Python script')
    parser.add_argument('--states',    required=True,  help='Comma-separated list of US state codes to process')
    parser.add_argument('--nodecount', required=False, help='The number of nodes in Azure Batch Pool', default='1')
    parser.add_argument('--ctask',     required=False, help='The name of the Task Blob Container', default='batchtask')
    parser.add_argument('--cin',       required=False, help='The name of the Input Blob Container', default='batchcsv')
    parser.add_argument('--cout',      required=False, help='The name of the Output Blob Container', default='batchlog')
    parser.add_argument('--clog',      required=False, help='The name of the Logging Blob Container', default='batchlog')
    parser.add_argument('--timeout',   required=False, help='Batch job timeout period in minutes', default='30')
    parser.add_argument('--dryrun',    required=False, help='Optionally specify y for dry-run mode with minimal task functionality', default='n')
    args = parser.parse_args()
    batch_client = StatesBatchClient(args)
    job_submitted = False

    try:
        # Add the (Python) Task script that will be executed on the Azure Batch nodes.
        batch_client.add_task_file(os.path.realpath(args.task))

        blobs_to_process = dict()
        states_to_process = (args.states).split(',')
        for st in states_to_process:
            blobname = 'postal_codes_{}.csv'.format(st.lower())
            blobs_to_process[blobname] = st

        blobs = batch_client.get_blobs(args.cin)
        for blob in blobs.items:
            if blob.name in blobs_to_process:
                print('adding blob: {}'.format(blob.name))
                batch_client.add_storage_input_blob(blob)

        if batch_client.blob_input_file_count() == len(blobs_to_process.keys()):
            job_submitted = True
            batch_client.execute()
            print('batch_client.execute() complete')
        else:
            print('mismatch of state codes given to actual blob names; job not submitted')
    except:
        print(sys.exc_info())
        traceback.print_exc()

    if job_submitted:
        try:
            input('Press ENTER to continue and delete the Job and the Pool...')
            batch_client.delete_job()
            batch_client.delete_pool()
            print('batch client script completed.')
        except:
            print(sys.exc_info())
            traceback.print_exc()
