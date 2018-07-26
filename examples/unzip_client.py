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
import batch_client

# Azure Batch client program which submits a job.
# Chris Joakim, Microsoft, 2018/06/12
#
# python unzip_client.py --pool UnzipPool --job unzip --task unzip_task.py

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pool',      required=True, help='The name of the Azure Batch Pool')
    parser.add_argument('--job',       required=True, help='The name of the Azure Batch Job')
    parser.add_argument('--task',      required=True, help='The name the Task Python script')
    parser.add_argument('--nodecount', required=False, help='The number of nodes in Azure Batch Pool', default='3')
    parser.add_argument('--ctask',     required=False, help='The name of the Task Blob Container', default='batchtask')
    parser.add_argument('--cin',       required=False, help='The name of the Input Blob Container', default='batchzips')
    parser.add_argument('--cout',      required=False, help='The name of the Output Blob Container', default='batchcsv')
    parser.add_argument('--clog',      required=False, help='The name of the Logging Blob Container', default='batchlog')
    parser.add_argument('--timeout',   required=False, help='Batch job timeout period in minutes', default='40')
    parser.add_argument('--outdir',    required=False, help='The name of the Local Output Directory', default='out')
    args = parser.parse_args()

    util = batch_client.BatchClient(args)  # Custom reusable class for Azure Batch use

    # Add the (Python) Task script that will be executed on the Azure Batch nodes.
    util.add_task_file(os.path.realpath(args.task))

    # Add the input zip files that will be uploaded and processed by the Task script in Azure.
    util.add_local_input_file(os.path.realpath('./data/NC1.zip'))
    util.add_local_input_file(os.path.realpath('./data/NC2.zip'))
    util.add_local_input_file(os.path.realpath('./data/NC3.zip'))

    util.execute()

    # The execute method looks like this:
    # def execute(self):
    #     timeout_minutes = int(self.args.timeout)
    #     self.upload_task_files(self.args.ctask, self.local_task_files)
    #     self.upload_local_input_files(self.args.cin, self.local_input_files)  
    #     self.create_pool()
    #     self.create_job()
    #     self.add_tasks()
    #     self.execute_tasks(timeout_minutes)

    print()
    input('Press ENTER to continue...')
    util.list_blobs(args.ctask)
    util.list_blobs(args.cin)
    util.list_blobs(args.cout)
    util.download_blobs_from_container(args.cout, args.outdir)
    util.delete_job()
    util.delete_pool()
    print('batch client script completed.')
