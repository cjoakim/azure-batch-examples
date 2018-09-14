from __future__ import print_function
import argparse
import datetime
import os
import sys
import time

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

sys.path.append('.')
import helpers
from batch_client import BatchClient

# Azure Batch client program which submits a job.
# Chris Joakim, Microsoft, 2018/09/13
#
# python csv_etl_client.py --pool CsvEtlPool --job csvetl --task csv_etl_task.py


class CsvEtlBatchClient(BatchClient):

    def __init__(self, args):
        BatchClient.__init__(self, args)

    def add_tasks(self):
        tasks = list()
        cin_container_sas_token  = self.get_container_sas_token(self.args.cin)
        cout_container_sas_token = self.get_container_sas_token(self.args.cout)
        permission = azureblob.models.BlobPermissions(read=True, add=True, create=True, write=True, delete=True)
        # permission=azureblob.BlobPermissions.READ
        docdbhost = os.environ["AZURE_COSMOSDB_DOCDB_URI"]
        docdbkey  = os.environ["AZURE_COSMOSDB_DOCDB_KEY"]

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

            template = 'python $AZ_BATCH_NODE_SHARED_DIR/{} --filepath {} --storageaccount {} --storagecontainer {} --sastoken "{}" --idx {} --docdbhost {} --docdbkey {} --dev false'
            command  = [
                template.format(
                    self.TASK_FILE,
                    blob.name,
                    self.STORAGE_ACCOUNT_NAME,
                    self.args.cin,
                    cin_container_sas_token,
                    str(idx),
                    docdbhost,
                    docdbkey)]
            #print(f'command: {command}')
            tasks.append(batch.models.TaskAddParameter(
                'task{}'.format(idx),
                helpers.wrap_commands_in_shell('linux', command),
                resource_files=[batchmodels.ResourceFile(
                                file_path=blob.name,
                                blob_source=sas_url)]))

        self.batch_client.task.add_collection(self.JOB_ID, tasks)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pool',      required=True, help='The name of the Azure Batch Pool')
    parser.add_argument('--job',       required=True, help='The name of the Azure Batch Job')
    parser.add_argument('--task',      required=True, help='The name the Task Python script')
    parser.add_argument('--nodecount', required=False, help='The number of nodes in Azure Batch Pool', default='1')
    parser.add_argument('--ctask',     required=False, help='The name of the Task Blob Container', default='batchtask')
    parser.add_argument('--cin',       required=False, help='The name of the Input Blob Container', default='batchcsv')
    parser.add_argument('--cout',      required=False, help='The name of the Output Blob Container', default='batchlog')
    parser.add_argument('--timeout',   required=False, help='Batch job timeout period in minutes', default='30')
    args = parser.parse_args()

    util = CsvEtlBatchClient(args)

    # Add the (Python) Task script that will be executed on the Azure Batch nodes.
    util.add_task_file(os.path.realpath(args.task))

    blobs = util.get_blobs(args.cin)
    for blob in blobs.items:
        if (blob.name).endswith('.csv'):
            print(f'blob: {blob.name}')
            util.add_storage_input_blob(blob)

    util.execute()

    print()
    input('Press ENTER to continue...')
    util.delete_job()
    util.delete_pool()
    print('batch client script completed.')
