#!/bin/bash

# Azure Batch Job Submission script.
# Chris Joakim, Microsoft, 2018/07/26

# Activate the Python virtual environment
source bin/activate

# Execute the Azure Batch Client Python script to submit a Batch Job
python states_client.py --pool StatesPool --job states --task states_task.py --states CT,FL,GA,MD,NC,SC,VA --nodecount 7 --dryrun n

echo 'done'

# These are the 7 files/states to be processed in parallel
# examples/data/postal_codes_ct.csv
# examples/data/postal_codes_fl.csv
# examples/data/postal_codes_ga.csv
# examples/data/postal_codes_md.csv
# examples/data/postal_codes_nc.csv
# examples/data/postal_codes_sc.csv
# examples/data/postal_codes_va.csv
# --states CT,FL,GA,MD,NC,SC,VA