"""
Usage:
  python explore.py write_blob
  python explore.py write_doc
  python explore.py write_eventhub
  python explore.py write_svcbus
  python explore.py excp_handling
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-python-how-to-use-queues

from __future__ import print_function
import argparse
import collections
import csv
import datetime
import json
import os
import string
import sys
import time
import uuid
import zipfile

from azure.servicebus import ServiceBusService

import azure.storage.blob as azureblob

import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors

# AZURE_EVENTHUB_LOGGING_CONN_STRING
# AZURE_EVENTHUB_LOGGING_KEY
# AZURE_EVENTHUB_LOGGING_POLICY
# AZURE_STORAGE_ACCOUNT
# AZURE_STORAGE_CONNECTION_STRING
# AZURE_STORAGE_KEY
# AZURE_STORAGE_SAS_KEY

def create_blob_client():
    acct = os.environ["AZURE_STORAGE_ACCOUNT"]
    key  = os.environ["AZURE_STORAGE_KEY"]
    print('create_blob_client: {} {}'.format(acct, key))
    return azureblob.BlockBlobService(account_name=acct, account_key=key)

def create_docdb_client(args):
    return document_client.DocumentClient(
        args.docdbhost, {'masterKey': args.docdbkey})

def create_eventhub_client(args):
    pass

def create_servicebus_client(args):
    key_name = 'RootManageSharedAccessKey' # SharedAccessKeyName from Azure portal
    key_value = '' # SharedAccessKey from Azure portal
    sbs = ServiceBusService(service_namespace,
                            shared_access_key_name=key_name,
                            shared_access_key_value=key_value)

# def write_blob(client, data):
#     blob_client.create_blob_from_path(
#         container,
#         output_file,
#         output_file_path)
#     return client.create_blob_from_text(container_name, blob_name, u'hello world')

def write_document(client, coll_link, data):
    doc = docdb_client.CreateDocument(coll_link, data)
    print("write_document: {} \n{}".format(coll_link, doc))

def write_eventhub_msg(client, obj):
    pass

def write_svcbus_msg(client, obj):
    pass

def write_log_data(blob_client, container, args, log_data):
    try:
        output_file = 'explore_{}.json'.format(str(args.idx))
        output_file_path = os.path.realpath(output_file)     
        log_json = json.dumps(log_data, sort_keys=True, indent=2)
        with open(output_file, 'w') as f:
            f.write(log_json)
        blob_client.create_blob_from_path(
            container,
            output_file,
            output_file_path)
    except KeyError:
        app_events.append('ERROR in write_log_data')

def base_evt():
    evt = dict()
    evt['pk'] = str(uuid.uuid1())
    evt['datetime'] = str(datetime.datetime.now())
    return evt

# the following functions are invoked from __main__

def write_blob_example():
    print('write_blob_example')
    client = create_blob_client()
    container = 'logging'
    print(client)

    # create a json string message
    evt = base_evt()
    evt['message'] = 'some blob message'
    evt['container'] = container
    jstr = json.dumps(evt, sort_keys=True, indent=2)
    print(evt)

    # save it to the blob storage container
    blob_name = evt['pk']
    print('writing blob: {} {} ...'.format(container, blob_name))
    b = client.create_blob_from_text(container, blob_name, jstr)
    print('blob: {}'.format(b.name))

    # list the blobs now in the container
    generator = client.list_blobs(container)
    for blob in generator:
        print('blob in container: {}'.format(blob.name))

def write_doc_example():
    print('write_doc_example')

    docdb_client = create_docdb_client(args)
    input_file = os.path.realpath(args.filepath)
    db_link    = 'dbs/dev'
    coll_link  = db_link + '/colls/zipdata'

def write_eventhub_example():
    print('write_eventhub_example')

def write_svcbus_example():
    print('write_svcbus_example')

def exception_handling_example():
    print('exception_handling_example')


if __name__ == '__main__':
    print(sys.argv)  # ['explore.py', 'write_blob']

    if len(sys.argv) > 0:
        func = sys.argv[1].lower()

        if func == 'write_blob':
            write_blob_example()

        elif func == 'write_doc':
            write_doc_example()

        elif func == 'write_eventhub':
            write_eventhub_example()

        elif func == 'write_svcbus':
            write_svcbus_example()

        elif func == 'excp_handling':
            exception_handling_example()
        else:
            print('unknown function: {}'.format(func))
    else:
        print('invalid args; no function')

        
