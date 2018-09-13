from __future__ import print_function
import argparse
import csv
import datetime
import json
import os
import sys
import time

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors

# https://github.com/Azure/azure-documentdb-python
# db: dev  collection: zipdata with /pk
# Chris Joakim, Microsoft, 2018/09/13

def create_client():
    host = os.environ["AZURE_COSMOSDB_DOCDB_URI"]
    key  = os.environ["AZURE_COSMOSDB_DOCDB_KEY"]
    print(f'host: {host}')
    print(f'key:  {key}')
    return document_client.DocumentClient(host, {'masterKey': key})

# python cosmosdb.py --func query_all_zipdata_docs
# python cosmosdb.py --func delete_all_zipdata_docs

# SELECT * FROM c where c.iata_code = "CIA"
# SELECT * FROM c where c.country = "ITALY"

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--func', required=True, help='The logical function to execute')
    args = parser.parse_args()
    print(args)

    if args.func == 'list_databases':
        client = create_client()
        print(client)
        databases = list(client.ReadDatabases())
        print(databases)
        db_link   = 'dbs/dev'
        coll_link = db_link + '/colls/zipdata'
        data = dict()
        data['pk'] = '1'
        data['epoch'] = int(time.time())
        doc = client.CreateDocument(coll_link, data)
        print(doc)

    elif args.func == 'create_doc':
        client = create_client()
        print(client)
        db_link   = 'dbs/dev'
        coll_link = db_link + '/colls/zipdata'
        data = dict()
        data['pk'] = '1'
        data['epoch'] = int(time.time())
        doc = client.CreateDocument(coll_link, data)
        print(doc)

    elif args.func == 'query_all_zipdata_docs':
        client = create_client()
        coll_link = 'dbs/dev/colls/zipdata'
        query = {
            'query': 'SELECT * FROM c'
        } 
        options = {
            'maxItemCount': 1200,
            'enableCrossPartitionQuery': True
        } 
        results = client.QueryDocuments(coll_link, query, options)
        doc_list = list()
        for idx, doc in enumerate(results):
            print(json.dumps(doc, sort_keys=True, indent=2))
            doc_list.append(doc)
        print(f'{len(doc_list)} documents found')

    elif args.func == 'query2':
        client = create_client()
        coll_link = 'dbs/dev/colls/zipdata'
        query = {
            'query': 'SELECT * FROM c where c.city_name = "Cary"'
        } 
        options = {
            'maxItemCount': 1200,
            'enableCrossPartitionQuery': True
        } 
        results = client.QueryDocuments(coll_link, query, options)
        for idx, doc in enumerate(results):
            print(json.dumps(doc, sort_keys=True, indent=2))

    elif args.func == 'delete_all_zipdata_docs':
        client = create_client()
        coll_link = 'dbs/dev/colls/zipdata'
        query = {
            'query': 'SELECT * FROM c'
        } 
        options = {
            'maxItemCount': 1200,
            'enableCrossPartitionQuery': True
        }
        id_list = list()
        results = client.QueryDocuments(coll_link, query, options)

        # collect the document id and pk pairs from the query
        for idx, doc in enumerate(results):
            d = dict()
            d['id'] = doc['id']
            d['pk'] = doc['pk']     
            id_list.append(d)
        print(f'{len(id_list)} documents found')

        # delete the documents using the id and pk pairs
        for d in id_list:
            options = { 'partitionKey': d['pk'] }
            doc_link = coll_link + '/docs/' + d['id']
            print(f'deleting document: {doc_link}')
            result = client.DeleteDocument(doc_link, options)

    elif args.func == 'csv':
        infile = '/Users/cjoakim/github/azure/local/batch/data/postalcodes/splits/NC9/split45.csv'
        with open(infile, 'rt') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            header = None
            for idx, row in enumerate(reader):
                print(str(idx))
                print(row)
                if idx < 1:
                    header = row
                    for fidx, field in enumerate(header):
                        print("{} {}".format(str(fidx), field))
                else:
                    data = dict()
                    for fidx, field in enumerate(header):
                        data[field] = row[fidx]
                    data['pk'] = data['city_name']
                    data['seq'] = data['id']
                    del data['id']
                    print(data)

        print(sys.version_info)
        # sys.version_info(major=3, minor=6, micro=1, releaselevel='final', serial=0)
