#!/bin/bash

# Create the Azure Blob Storage Containers for the Azure Batch Jobs.
# Chris Joakim, Microsoft, 2018/06/10

# Display the default storage account name and key
echo $AZURE_STORAGE_ACCOUNT
echo $AZURE_STORAGE_KEY

echo 'deleting blob storage containers with the CLI...'
az storage container delete --name batchtask
az storage container delete --name batchzips
az storage container delete --name batchcsv
sleep 30

echo 'creating blob storage containers with the CLI...'
az storage container create --name batchtask
az storage container create --name batchzips
az storage container create --name batchcsv
sleep 5

echo 'deleting all documents in the DocumentDB zipdata collection with the Python SDK...'
source bin/activate
python cosmosdb.py --func delete_all_zipdata_docs

echo 'done'
