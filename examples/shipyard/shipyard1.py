"""
Usage:
  python shipyard1.py --function write_blob
  python shipyard1.py --function write_doc
  python shipyard1.py --function write_eventhub
  python shipyard1.py --function write_svcbus
  python shipyard1.py --function excp_handling
  python shipyard1.py --function download_logging_blobs
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# Simple python program, to be executed on a workstation, which demonstrates how
# to access Azure Blob storage, CosmosDB, EventHub, and ServiceBus.
# Chris Joakim, Microsoft, 2018/07/26

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

from azure.servicebus import ServiceBusService, Message, Queue

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

def create_servicebus_client():
    namespace = os.environ["AZURE_SERVICEBUS_NAMESPACE"]   # example: cjoakimservicebus
    key_name  = os.environ["AZURE_SERVICEBUS_KEY_NAME"]    # example: RootManageSharedAccessKey
    key_value = os.environ["AZURE_SERVICEBUS_ACCESS_KEY"]  # example: YiFRcE..............................Ifobwhg=
    return ServiceBusService(
        service_namespace=namespace,
        shared_access_key_name=key_name,
        shared_access_key_value=key_value)

def base_evt():
    evt = dict()
    evt['pk'] = str(uuid.uuid1())
    evt['datetime'] = str(datetime.datetime.now())
    return evt

def remote_log(blob_client, evthub_sender, svcbus_client, evt):
    evt_id = evt['pk']
    jstr = json.dumps(evt)
    queue = os.environ["AZURE_SERVICEBUS_QUEUE"]
    print('remote_log: {}'.format(jstr))

    # yes, this is overkill - logging to three different sinks
    blob_client.create_blob_from_text('logging', evt_id, jstr)
    evthub_sender.send(EventData(jstr))
    svcbus_client.send_queue_message(queue, Message(jstr))

# the following functions are invoked from __main__

def write_blob_example():
    # see https://azure.github.io/azure-storage-python/ref/azure.storage.blob.models.html
    print('write_blob_example')
    client = create_blob_client()
    container = 'logging'
    print(client)

    # create a json string message
    evt = base_evt()
    evt['message'] = 'some message from shipyard1 for blob storage'
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
    evt['message'] = 'some message from shipyard1 for cosmosdb'
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
        for i in range(10):
            # create and send a message
            evt = base_evt()
            evt['message'] = 'some message from shipyard1 for eventhub'
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
    client = create_servicebus_client()
    queue  = os.environ["AZURE_SERVICEBUS_QUEUE"]  # example: logging

    for i in range(10):
        evt = base_evt()
        evt['message'] = 'some message from shipyard1 for servicebus'
        evt['sequence'] = i
        jstr = json.dumps(evt)
        msg = Message(jstr)
        client.send_queue_message(queue, msg)
        print('sent message number {} {}'.format(i, jstr))

def exception_handling_example():
    print('exception_handling_example - logging to remote sinks')

    # initialize the three logging sinks before processing the input
    blob_client   = create_blob_client()
    svcbus_client = create_servicebus_client()
    evthub_client = create_eventhub_client()
    evthub_sender = evthub_client.add_sender()
    evthub_client.run()

    evt = None
    try:
        # use a range instead of a file for demonstration purposes
        range_size = 20
        last_item = range_size - 1
        for i in range(range_size):  
            evt = None
            if i == 0:
                evt = base_evt()
                evt['message'] = 'start of the job'
                evt['sequence'] = i
                remote_log(blob_client, evthub_sender, svcbus_client, evt)

            elif i == last_item:
                evt = base_evt()
                evt['message'] = 'end of the job'
                evt['sequence'] = i
                remote_log(blob_client, evthub_sender, svcbus_client, evt)

            elif i == 13:
                evt = base_evt()
                evt['message'] = 'warning, unlucky number encountered!'
                evt['sequence'] = i
                remote_log(blob_client, evthub_sender, svcbus_client, evt)

            else:
                pass 
    except:
        print('execption encountered, lets log it three ways!')
        evt = base_evt()
        evt['message'] = 'exception encountered'
        evt['exception'] = str(sys.exc_info()[1])
    finally:
        print('stopping evthub_client...')
        evthub_client.stop()
        print('evthub_client stopped')

def download_logging_blobs():
    client = create_blob_client()
    container = 'logging'

    # list the blobs now in the container
    generator = client.list_blobs(container)
    for blob in generator:
        outfile = 'tmp/{}'.format(blob.name)
        print('downloading container: {} blob: {} to file: {}'.format(container, blob.name, outfile))
        client.get_blob_to_path(container, blob.name, outfile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--function', required=True, help='The logical function to execute')
    args = parser.parse_args()

    if args.function == 'write_blob':
        write_blob_example()

    elif args.function == 'write_doc':
        write_doc_example()

    elif args.function == 'write_eventhub':
        write_eventhub_example()

    elif args.function == 'write_svcbus':
        write_svcbus_example()

    elif args.function == 'excp_handling':
        exception_handling_example()

    elif args.function == 'download_logging_blobs':
        download_logging_blobs()

    else:
        print('unknown function: {}'.format(args.function))

