#!/bin/bash

# Azure Batch Job Submission script.
# Chris Joakim, Microsoft, 2018/09/13

# Activate the Python virtual environment
source bin/activate

rm -rf tmp/csvetl*

echo 'Start Date:'
date

# Exeucte the Azure Batch Client Python script to submit a Batch Job
python csv_etl_client.py --pool csv_etl_pool --job csvetl --task csv_etl_task.py --nodecount 1

echo 'Finish Date:'
date

echo 'done'
