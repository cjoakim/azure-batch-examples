#!/bin/bash

# Azure Batch Job Submission script.
# Chris Joakim, Microsoft, 2018/06/13

# Activate the Python virtual environment
source bin/activate

date

# Exeucte the Azure Batch Client Python script to submit a Batch Job
# python states_client.py --pool StatesPool --job states --task states_task.py --states CT,FL,GA,MD,NC,SC,VA --nodecount 7 --submit n
python states_client.py --pool StatesPool --job states --task states_task.py --states CT --nodecount 1 --submit y

date

echo 'done'

# These are the 7 files/states to be processed in parallel
# examples/data/postal_codes_ct.csv
# examples/data/postal_codes_fl.csv
# examples/data/postal_codes_ga.csv
# examples/data/postal_codes_md.csv
# examples/data/postal_codes_nc.csv
# examples/data/postal_codes_sc.csv
# examples/data/postal_codes_va.csv

# (examples) [~/github/azure-batch-examples/examples]$ ./states_job.sh
# create_container: states
# create_container: batchtask
# create_container: batchcsv
# create_container: batchlog
# adding blob: postal_codes_ct.csv
# adding blob: postal_codes_fl.csv
# Uploading file "/Users/cjoakim/github/azure-batch-examples/examples/states_task.py" to container "batchtask"...
# no input files specified for upload
# Creating pool "StatesPool1528838766"...
# Creating job "states1528838766"...
# command: ['python $AZ_BATCH_NODE_SHARED_DIR/states_task.py --filepath postal_codes_ct.csv --storageaccount cjoakimstdstorage --storagecontainer batchcsv --sastoken "se=2018-06-12T23%3A26%3A08Z&sp=w&sv=2017-04-17&sr=c&sig=ryekDDb0WEwVlqC2vdmQ8aUFSDNbh3zqaup%2BmZJ%2B7po%3D" --idx 0']
# command: ['python $AZ_BATCH_NODE_SHARED_DIR/states_task.py --filepath postal_codes_fl.csv --storageaccount cjoakimstdstorage --storagecontainer batchcsv --sastoken "se=2018-06-12T23%3A26%3A08Z&sp=w&sv=2017-04-17&sr=c&sig=ryekDDb0WEwVlqC2vdmQ8aUFSDNbh3zqaup%2BmZJ%2B7po%3D" --idx 1']
# Monitoring all tasks for 'Completed' state, timeout in 0:30:00.........................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................
# (<class 'RuntimeError'>, RuntimeError("ERROR: Tasks did not reach 'Completed' state within timeout period of 0:30:00",), <traceback object at 0x10bbc6748>)
# Traceback (most recent call last):
#   File "states_client.py", line 103, in <module>
#     util.execute()
#   File "/Users/cjoakim/github/azure-batch-examples/examples/batch_client.py", line 191, in execute
#     self.execute_tasks(timeout_minutes)
#   File "/Users/cjoakim/github/azure-batch-examples/examples/batch_client.py", line 279, in execute_tasks
#     raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within timeout period of " + str(timeout))
# RuntimeError: ERROR: Tasks did not reach 'Completed' state within timeout period of 0:30:00
# Press ENTER to continue and delete the Job and the Pool...
# batch client script completed.
# done
