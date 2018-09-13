#!/bin/bash

# Azure Batch Job Submission script.
# Chris Joakim, Microsoft, 2018/09/13

echo 'removing the previous tmp/unzip- stdout and stderr files..'
rm tmp/unzip*

source bin/activate

echo 'executing azure batch client to submit batch job...'
python unzip_client.py --pool UnzipPool --job unzip --task unzip_task.py

echo 'listing the blob containers with the CLI, redirecting to out/...'
rm out/batch*.json
az storage blob list -c batchtask > out/batchtask.json
az storage blob list -c batchzips > out/batchzips.json
az storage blob list -c batchcsv  > out/batchcsv.json

echo 'done'
