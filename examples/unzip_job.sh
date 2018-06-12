#!/bin/bash

# Azure Batch Job Submission script.
# Chris Joakim, Microsoft, 2018/06/10

echo 'executing azure batch client to submit batch job...'
source bin/activate

python unzip_client.py --pool UnzipPool --job unzip --task unzip_task.py

echo 'listing the blob containers with the CLI, redirecting to out/...'
rm out/batch*.json
az storage blob list -c batchtask > out/batchtask.json
az storage blob list -c batchzips > out/batchzips.json
az storage blob list -c batchcsv  > out/batchcsv.json

echo 'done'
