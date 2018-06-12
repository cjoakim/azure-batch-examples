from __future__ import print_function
import argparse
import datetime
import json
import os
import sys
import time

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

sys.path.append('.')
sys.path.append('..')
import helpers


# Reusable code for submitting Azure Batch jobs.
# Class BatchUtil may be used "as is", or extended/inherited.
# Chris Joakim, Microsoft, 2018/06/10

class BatchUtil(object):

    def __init__(self, args):
        try:
            self.args = args
            self.epoch = int(time.time())
            self.blob_client  = None
            self.batch_client = None
            self.local_task_files  = list()
            self.local_input_files = list()
            self.blob_task_files   = list()
            self.blob_input_files  = list()
            self.BATCH_ACCOUNT_NAME   = os.environ["AZURE_BATCH_ACCOUNT"]
            self.BATCH_ACCOUNT_KEY    = os.environ["AZURE_BATCH_KEY"]
            self.BATCH_ACCOUNT_URL    = os.environ["AZURE_BATCH_URL"]
            self.STORAGE_ACCOUNT_NAME = os.environ["AZURE_STORAGE_ACCOUNT"]
            self.STORAGE_ACCOUNT_KEY  = os.environ["AZURE_STORAGE_KEY"]
            self.SUFFIX            = '{}'.format(self.epoch)
            self.POOL_ID           = '{}{}'.format(args.pool, self.epoch)
            self.POOL_NODE_COUNT   = int(self.args.nodecount)
            self.POOL_VM_SIZE      = 'Standard_DS3_v2'  # Standard_A4 BASIC_A1
            self.NODE_OS_PUBLISHER = 'Canonical'
            self.NODE_OS_OFFER     = 'UbuntuServer'
            self.NODE_OS_SKU       = '16'
            self.TASK_FILE         = args.task
            self.JOB_ID            = '{}{}'.format(args.job, self.epoch).lower()
            self.JOB_CONTAINER     = 'job-' + self.JOB_ID.lower()
            self.create_blob_client()
            self.create_batch_service_client()
            self.create_container(args.job)
            self.create_container(args.ctask)
            self.create_container(args.cin)
            self.create_container(args.cout)
        except:
            print("Unexpected error in BatchUtil constructor: ", sys.exc_info()[0])
        self.log_state()
            
    def current_state(self):
        state = dict()
        for arg in vars(self.args):
            key = 'arg {}'.format(arg)
            state[key] = getattr(self.args, arg)

        state['SUFFIX']             = self.SUFFIX
        state['POOL_ID']            = self.POOL_ID
        state['POOL_NODE_COUNT']    = self.POOL_NODE_COUNT
        state['POOL_VM_SIZE']       = self.POOL_VM_SIZE
        state['NODE_OS_PUBLISHER']  = self.NODE_OS_PUBLISHER
        state['NODE_OS_OFFER']      = self.NODE_OS_OFFER
        state['NODE_OS_SKU']        = self.NODE_OS_SKU
        state['TASK_FILE']          = self.TASK_FILE
        state['JOB_ID']             = self.JOB_ID
        state['JOB_CONTAINER']      = self.JOB_CONTAINER
        state['epoch']              = self.epoch
        state['blob_client']        = str(self.blob_client)
        state['batch_client']       = str(self.batch_client)
        state['local_task_files']   = self.local_task_files
        state['local_input_files']  = self.local_input_files
        state['blob_task_files']    = self.blob_task_files
        state['blob_input_files']   = self.blob_input_files
        return state

    def log_state(self):
        print(json.dumps(self.current_state(), sort_keys=True, indent=2))

    def create_blob_client(self):
        self.blob_client = azureblob.BlockBlobService(
            account_name=self.STORAGE_ACCOUNT_NAME,
            account_key=self.STORAGE_ACCOUNT_KEY)
        return self.blob_client

    def create_batch_service_client(self):
        credentials = batchauth.SharedKeyCredentials(
            self.BATCH_ACCOUNT_NAME,
            self.BATCH_ACCOUNT_KEY)
        self.batch_client = batch.BatchServiceClient(
            credentials,
            base_url=self.BATCH_ACCOUNT_URL)
        return self.batch_client

    def add_task_file(self, file_path):
        if file_path:
            self.local_task_files.append(file_path)

    def add_local_input_file(self, file_path):
        if file_path:
            self.local_input_files.append(file_path)

    def add_storage_input_blob(self, f):
        if f:
            self.blob_input_files.append(f)

    def query_yes_no(self, question, default="yes"):
        valid = {'y': 'yes', 'n': 'no'}
        if default is None:
            prompt = ' [y/n] '
        elif default == 'yes':
            prompt = ' [Y/n] '
        elif default == 'no':
            prompt = ' [y/N] '
        else:
            raise ValueError("Invalid default answer: '{}'".format(default))

        while 1:
            choice = input(question + prompt).lower()
            if default and not choice:
                return default
            try:
                return valid[choice[0]]
            except (KeyError, IndexError):
                print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

    def upload_task_files(self, container, file_paths):
        if len(file_paths) > 0:
            for file_path in file_paths:
                f = self.upload_file_to_container(container, file_path)
                self.blob_task_files.append(f)
        else:
            print('no task files to upload!')

    def upload_local_input_files(self, container, file_paths):
        if len(file_paths) > 0:
            for file_path in file_paths:
                f = self.upload_file_to_container(container, file_path)
                self.blob_input_files.append(f)
        else:
            print('no input files to upload!')

    def upload_file_to_container(self, container_name, file_path):
        blob_name = os.path.basename(file_path)
        print('Uploading file "{}" to container "{}"...'.format(file_path, container_name))

        self.blob_client.create_blob_from_path(
            container_name,
            blob_name,
            file_path)

        sas_token = self.blob_client.generate_blob_shared_access_signature(
            container_name,
            blob_name,
            permission=azureblob.BlobPermissions.READ,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

        sas_url = self.blob_client.make_blob_url(
            container_name,
            blob_name,
            sas_token=sas_token)

        return batchmodels.ResourceFile(
            file_path=blob_name,
            blob_source=sas_url)

    def get_container_sas_token(self, container_name, permission=azureblob.BlobPermissions.WRITE):
        token = self.blob_client.generate_container_shared_access_signature(
                container_name,
                permission=permission,
                expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))
        return token

    def execute(self):
        timeout_minutes = int(self.args.timeout)
        self.upload_task_files(self.args.ctask, self.local_task_files)
        self.upload_local_input_files(self.args.cin, self.local_input_files)  
        self.create_pool()
        self.create_job()
        self.add_tasks()
        self.execute_tasks(timeout_minutes)

    def create_pool(self, opts={}):
        print('Creating pool "{}"...'.format(self.POOL_ID))
        task_commands = [
            'cp -p {} $AZ_BATCH_NODE_SHARED_DIR'.format(self.TASK_FILE),
            'curl -fSsL https://bootstrap.pypa.io/get-pip.py | python',
            'pip install azure-storage==0.32.0',
            'pip install pydocumentdb==2.2.0'
        ]

        sku_to_use, image_ref_to_use = \
            helpers.select_latest_verified_vm_image_with_node_agent_sku(
                self.batch_client, self.NODE_OS_PUBLISHER, self.NODE_OS_OFFER, self.NODE_OS_SKU)

        user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=batchmodels.ElevationLevel.admin)

        new_pool = batch.models.PoolAddParameter(
            id=self.POOL_ID,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=image_ref_to_use,
                node_agent_sku_id=sku_to_use),
            vm_size=self.POOL_VM_SIZE,
            target_dedicated_nodes=self.POOL_NODE_COUNT,
            start_task=batch.models.StartTask(
                command_line=helpers.wrap_commands_in_shell('linux', task_commands),
                user_identity=batchmodels.UserIdentity(auto_user=user),
                wait_for_success=True,
                resource_files=self.blob_task_files),
        )
        try:
            self.batch_client.pool.add(new_pool)
        except batchmodels.batch_error.BatchErrorException as err:
            self.print_batch_exception(err)
            raise

    def create_job(self):
        print('Creating job "{}"...'.format(self.JOB_ID))
        job = batch.models.JobAddParameter(
            self.JOB_ID, batch.models.PoolInformation(pool_id=self.POOL_ID))
        try:
            self.batch_client.job.add(job)
        except batchmodels.batch_error.BatchErrorException as err:
            self.print_batch_exception(err)
            raise

    def add_tasks(self):
        tasks = list()
        sas_token = self.get_container_sas_token(self.args.cout)

        for idx, input_file in enumerate(self.blob_input_files):
            template = 'python $AZ_BATCH_NODE_SHARED_DIR/{} --filepath {} --storageaccount {} --storagecontainer {} --sastoken "{}" --dev false'
            command  = [
                template.format(
                    self.TASK_FILE,
                    input_file.file_path,
                    self.STORAGE_ACCOUNT_NAME,
                    self.args.cout,
                    sas_token)]
            tasks.append(batch.models.TaskAddParameter(
                'task{}'.format(idx),
                helpers.wrap_commands_in_shell('linux', command),
                resource_files=[input_file]))

        self.batch_client.task.add_collection(self.JOB_ID, tasks)

    def execute_tasks(self, timeout_minutes):
        timeout = datetime.timedelta(minutes=timeout_minutes)
        timeout_expiration = datetime.datetime.now() + timeout
        print("Monitoring all tasks for 'Completed' state, timeout in {}...".format(timeout), end='')

        while datetime.datetime.now() < timeout_expiration:
            print('.', end='')
            sys.stdout.flush()
            tasks = self.batch_client.task.list(self.JOB_ID)

            incomplete_tasks = [task for task in tasks if
                                task.state != batchmodels.TaskState.completed]
            if not incomplete_tasks:
                print()
                return True
            else:
                time.sleep(1)
        print()
        raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within timeout period of " + str(timeout))

    def create_container(self, container_name, fail_on_exist=False):
        print(f'create_container: {container_name}')
        self.blob_client.create_container(container_name, fail_on_exist=fail_on_exist)

    def delete_container(self, container_name):
        print(f'delete_container: {container_name}')
        self.blob_client.delete_container(container_name)

    def get_blobs(self, container_name):
        return self.blob_client.list_blobs(container_name)

    def list_blobs(self, container_name):
        blobs = self.blob_client.list_blobs(container_name)
        for blob in blobs.items:
            print(f'container: {container_name}  blob: {blob.name}')

    def download_blobs_from_container(self, container_name, directory_path):
        print(f'Downloading all files from container {container_name}')
        blobs = self.blob_client.list_blobs(container_name)
        for blob in blobs.items:
            destination_file_path = os.path.join(directory_path, blob.name)
            self.blob_client.get_blob_to_path(container_name, blob.name, destination_file_path)
            print(f'Downloaded blob {blob.name} from container {container_name} to {destination_file_path}')
        print('Blob download complete')

    def delete_job(self, job_id=None):
        if job_id == None:
            job_id = self.JOB_ID
        self.batch_client.job.delete(job_id)

    def delete_pool(self, pool_id=None):
        if pool_id == None:
            pool_id = self.POOL_ID
        self.batch_client.pool.delete(pool_id)

    def print_batch_exception(self, batch_exception):
        print('-------------------------------------------')
        print('Exception encountered:')
        if batch_exception.error and \
                batch_exception.error.message and \
                batch_exception.error.message.value:
            print(batch_exception.error.message.value)
            if batch_exception.error.values:
                print()
                for mesg in batch_exception.error.values:
                    print('{}:\t{}'.format(mesg.key, mesg.value))
        print('-------------------------------------------')
