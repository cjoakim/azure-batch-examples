#!/bin/bash

# Azure Batch Job Submission script.
# Chris Joakim, Microsoft, 2018/09/13

echo 'removing the previous tmp/unzip- stdout and stderr files..'
rm tmp/unzip*

source bin/activate

date

python unzip_client.py --function execute  --pool unzip_pool --job unzip --task unzip_task.py

date

echo 'listing the blob containers with the CLI, redirecting to out/...'
rm out/batch*.json
az storage blob list -c batchtask > out/batchtask.json
az storage blob list -c batchzips > out/batchzips.json
az storage blob list -c batchcsv  > out/batchcsv.json

echo 'done'
