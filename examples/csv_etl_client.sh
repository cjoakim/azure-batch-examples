#!/bin/bash

# Azure Batch Job Submission script.
# Chris Joakim, Microsoft, 2018/06/13

# Activate the Python virtual environment
source bin/activate

# Exeucte the Azure Batch Client Python script to submit a Batch Job
python csv_etl_client.py --pool CsvEtlPool --job csvetl --task csv_etl_task.py --nodecount 5

echo 'done'
