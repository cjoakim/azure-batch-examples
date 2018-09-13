from __future__ import print_function
import argparse
import datetime
import io
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

STANDARD_OUT_FILE_NAME = 'stdout.txt'
STANDARD_ERR_FILE_NAME = 'stderr.txt'

# Reusable superclass for submitting Azure Batch jobs.
# Class BatchClient may be used "as is", or extended/inherited.
# Chris Joakim, Microsoft, 2018/09/13

class BatchClient(object):

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
            self.POOL_ID           = '{}_{}'.format(args.pool, self.epoch).lower()
            self.POOL_NODE_COUNT   = int(self.args.nodecount)
            self.TASK_FILE         = args.task
            self.JOB_ID            = '{}-{}'.format(args.job, self.epoch).lower()
            self.JOB_CONTAINER     = 'job-' + self.JOB_ID.lower()
            self.set_pool_os_and_size()
            self.create_blob_client()
            self.create_batch_service_client()
            self.create_container(args.job)
            self.create_container(args.ctask)
            self.create_container(args.cin)
            self.create_container(args.cout)
        except:
            print("Unexpected error in BatchClient constructor: ", sys.exc_info()[0])
    
    def execute(self):
        try:
            timeout_minutes = int(self.args.timeout)
            self.upload_task_files(self.args.ctask, self.local_task_files)
            self.upload_local_input_files(self.args.cin, self.local_input_files)  
            self.create_pool()
            self.create_job()
            self.add_tasks()
            self.execute_tasks(timeout_minutes)
            self.capture_stdout_stderr_streams()

        except batchmodels.batch_error.BatchErrorException as err:
            print_batch_exception(err)
            raise
        except Exception:
            print(sys.exc_info()[1])

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

    def blob_input_file_count(self):
        return len(self.blob_input_files)

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
            print('no input files specified for upload')

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

    def set_pool_os_and_size(self):
        # subclasses can override this method
        self.NODE_OS_PUBLISHER = 'Canonical'
        self.NODE_OS_OFFER     = 'UbuntuServer'
        self.NODE_OS_SKU       = '16'
        self.POOL_VM_SIZE      = 'Standard_DS3_v2'

        # Other sizes: Basic_A4, Standard_DS3_v2, Standard_DS2_v2, etc
        # see list_vm_sizes.sh

    def use_windows_server(self):
        pass  # implement similarly to 'use_canonical_ubuntu'

    def create_pool(self, opts={}):
        print('Creating pool "{}"...'.format(self.POOL_ID))
        task_commands = [
            'cp -p {} $AZ_BATCH_NODE_SHARED_DIR'.format(self.TASK_FILE),
            'curl -fSsL https://bootstrap.pypa.io/get-pip.py | python',
            'pip install azure-storage==0.36.0',
            'pip install pydocumentdb==2.3.3',
            'pip install pandas==0.23.4'
        ]

        sku_to_use, image_ref_to_use = \
            self.select_latest_verified_vm_image_with_node_agent_sku(
                self.batch_client, self.NODE_OS_PUBLISHER, self.NODE_OS_OFFER, self.NODE_OS_SKU)
        print('sku:   {}'.format(sku_to_use))
        print('image: {}'.format(image_ref_to_use))

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
                command_line=self.wrap_commands_in_shell('linux', task_commands),
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
        # subclasses should generally override this method, except for simple cases
        tasks = list()
        sas_token_cout = self.get_container_sas_token(self.args.cout)  # output container sas token
        sas_token_clog = self.get_container_sas_token(self.args.clog)  # logging container sas token

        for idx, input_file in enumerate(self.blob_input_files):
            template = "python $AZ_BATCH_NODE_SHARED_DIR/{} --filepath {} --storageaccount {} --outputcontainer {} --outputtoken {} --loggingcontainer {} --loggingtoken {} --dev false"
            command  = [
                template.format(
                    self.TASK_FILE,
                    input_file.file_path,
                    self.STORAGE_ACCOUNT_NAME,
                    self.args.cout,
                    sas_token_cout,
                    self.args.clog,
                    sas_token_clog)]
            print('add_tasks, command: {}'.format(command))
            tasks.append(batch.models.TaskAddParameter(
                'task{}'.format(idx),
                self.wrap_commands_in_shell('linux', command),
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
                time.sleep(10)
        print("ERROR: Tasks did not reach 'Completed' state within timeout period of " + str(timeout))

    def capture_stdout_stderr_streams(self, encoding=None):
        print('capture_stdout_stderr_streams...')

        tasks = self.batch_client.task.list(self.JOB_ID)
        for task in tasks:
            node_id = self.batch_client.task.get(self.JOB_ID, task.id).node_info.node_id
            print("Task: {}".format(task.id))
            print("Node: {}".format(node_id))
            stream_names = [STANDARD_OUT_FILE_NAME, STANDARD_ERR_FILE_NAME]
            for stream_name in stream_names:
                output_file = 'tmp/{}-{}-{}-{}'.format(self.JOB_ID, task.id, self.epoch, stream_name)
                stream = self.batch_client.file.get_from_task(self.JOB_ID, task.id, stream_name)
                stream_text = self.read_stream_as_string(stream, encoding)
                with open(output_file, 'w') as f:
                    f.write(stream_text)
                    print("stream file written {}:".format(output_file))

    def read_stream_as_string(self, stream, encoding):
        output = io.BytesIO()
        try:
            for data in stream:
                output.write(data)
            if encoding is None:
                encoding = 'utf-8'
            return output.getvalue().decode(encoding)
        finally:
            output.close()
        raise RuntimeError('RuntimeError in read_stream_as_string on {}'.format(stream))

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

    def select_latest_verified_vm_image_with_node_agent_sku(
            self, batch_client, publisher, offer, sku_starts_with):
        # get verified vm image list and node agent sku ids from service
        node_agent_skus = batch_client.account.list_node_agent_skus()
        # pick the latest supported sku
        skus_to_use = [
            (sku, image_ref) for sku in node_agent_skus for image_ref in sorted(
                sku.verified_image_references, key=lambda item: item.sku)
            if image_ref.publisher.lower() == publisher.lower() and
            image_ref.offer.lower() == offer.lower() and
            image_ref.sku.startswith(sku_starts_with)
        ]
        # skus are listed in reverse order, pick first for latest
        sku_to_use, image_ref_to_use = skus_to_use[0]
        return (sku_to_use.id, image_ref_to_use)

    def wrap_commands_in_shell(self, ostype, commands):
        if ostype.lower() == 'linux':
            #return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(';'.join(commands))
            cmds = "/bin/bash set -e; set -o pipefail; {}; wait".format(';'.join(commands))
            print('wrap_commands_in_shell: {}'.format(cmds))
            return cmds
        
        elif ostype.lower() == 'windows':
            return 'cmd.exe /c "{}"'.format('&'.join(commands))
        else:
            raise ValueError('unknown ostype: {}'.format(ostype))

    def current_state(self):
        state = dict()
        for arg in vars(self.args):
            key = 'args.{}'.format(arg)
            state[key] = getattr(self.args, arg)

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
