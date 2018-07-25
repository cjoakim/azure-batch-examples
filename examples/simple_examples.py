"""
Usage:
  python simple_examples.py write_blob
  python simple_examples.py write_doc
  python simple_examples.py write_eventhub
  python simple_examples.py write_svcbus
  python simple_examples.py excp_handling
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

from azure.eventhub import EventHubClient, Sender, EventData

from azure.servicebus import ServiceBusService

import azure.storage.blob as azureblob

import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client


def create_blob_client():
    acct = os.environ["AZURE_STORAGE_ACCOUNT"]
    key  = os.environ["AZURE_STORAGE_KEY"]
    print('create_blob_client: {} {}'.format(acct, key))
    return azureblob.BlockBlobService(account_name=acct, account_key=key)

def create_docdb_client():
    host = os.environ["AZURE_COSMOSDB_DOCDB_URI"]
    key  = os.environ["AZURE_COSMOSDB_DOCDB_KEY"]
    return document_client.DocumentClient(host, {'masterKey': key})

def create_eventhub_client():
    # see https://github.com/Azure/azure-event-hubs-python/blob/master/examples/send.py
    namespace = os.environ["AZURE_EVENTHUB_NAMESPACE"]   # example: cjoakimeventhubs
    hubname   = os.environ["AZURE_EVENTHUB_HUBNAME"]     # example: logging
    username  = os.environ["AZURE_EVENTHUB_LOGGING_POLICY"]  # example: SendLoggingPolicy
    password  = os.environ["AZURE_EVENTHUB_LOGGING_KEY"]  # example: vEOr0S9b2........................zf/y/x5bME=
    address   = 'amqps://{}.servicebus.windows.net/{}'.format(namespace, hubname)
    return EventHubClient(address, debug=False, username=username, password=password)

def create_servicebus_client(args):
    key_name = 'RootManageSharedAccessKey' # SharedAccessKeyName from Azure portal
    key_value = '' # SharedAccessKey from Azure portal
    sbs = ServiceBusService(service_namespace,
                            shared_access_key_name=key_name,
                            shared_access_key_value=key_value)


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
    # see https://azure.github.io/azure-storage-python/ref/azure.storage.blob.models.html
    print('write_blob_example')
    client = create_blob_client()
    container = 'logging'
    print(client)

    # create a json string message
    evt = base_evt()
    evt['message'] = 'some message for blob storage'
    evt['container'] = container
    jstr = json.dumps(evt, sort_keys=True, indent=2)
    print(evt)

    # write the json blob to the blob storage container
    blob_name = evt['pk']
    print('writing blob: {} {} ...'.format(container, blob_name))
    client.create_blob_from_text(container, blob_name, jstr)

    # list the blobs now in the container
    generator = client.list_blobs(container)
    for blob in generator:
        # blob is an instance of azure.storage.blob.models.Blob
        # blob.properties is an instance of azure.storage.blob.models.BlobProperties
        btype = blob.properties.blob_type
        bsize = blob.properties.content_length
        lmod  = blob.properties.last_modified
        print('blob in container; name: {} type: {} size: {} mod: {}'.format(blob.name, btype, bsize, lmod))

def write_doc_example():
    print('write_doc_example')
    client = create_docdb_client()
    db_name, coll_name = 'dev', 'logging'
    coll_link  = 'dbs/dev/colls/{}'.format(coll_name)

    # create a message
    evt = base_evt()
    evt['message'] = 'some message for cosmosdb'
    evt['database'] = db_name
    evt['collection'] = coll_name
    print(json.dumps(evt, sort_keys=True, indent=2))

    doc = client.CreateDocument(coll_link, evt)
    print("write_document: {} \n{}".format(coll_link, doc))

def write_eventhub_example():
    print('write_eventhub_example')
    client = create_eventhub_client()
    sender = client.add_sender()  # partition="1")
    # :param partition:
    # Optionally specify a particular partition to send to.
    # If omitted, the events will be distributed to available partitions via round-robin.

    client.run()  # <- remember to stop() the client when done

    try:
        for i in range(20):
            # create and send a message
            evt = base_evt()
            evt['message'] = 'some message for eventhub'
            evt['sequence'] = i
            jstr = json.dumps(evt)
            sender.send(EventData(jstr))
            print('sent message number {} {}'.format(i, jstr))
    except:
        raise
    finally:
        print('stopping client...')
        client.stop()
        print('client stopped')

def write_svcbus_example():
    print('write_svcbus_example')

def exception_handling_example():
    print('exception_handling_example')


if __name__ == '__main__':
    print(sys.argv)  # ['simple_examples.py', 'write_blob']

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

        
