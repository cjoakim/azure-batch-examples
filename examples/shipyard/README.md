# Azure Batch with Shipyard

Azure Batch compute service examples with Shipyard - containerized workloads

# Links

- https://docs.microsoft.com/en-us/azure/batch/batch-technical-overview
- https://azure.github.io/batch-shipyard/
- https://github.com/Azure/batch-shipyard
- https://batch-shipyard.readthedocs.io/en/latest/00-introduction/

---

# What is Azure Batch?

- Azure Batch Compute PaaS service
- Specify your OS, VM size, number of VMs
- Compute exists only for the duration of of your submitted batch jobs; low cost
- Excellent for Parallel and HPC workloads
- Has a Client and Server-side SDK in multiple programming languages (Python, Java, etc)

## Alternatives?

- Data Science Virtual Machine
- Azure Data Lake Analytics
- Azure Databricks 

---

# Why use Shipyard?

- Containerize your tasks in Docker images
- Test the images locally on your workstation
- No configuration differences - what runs in Batch Shipyard is exactly your Docker image
- Shipyard interacts with the Azure Batch PaaS service via simpler yaml config files
- Run anything in your Docker images in Batch - Python, Java, R, etc

---

# Using Docker and Shipyard

Simple example: given CSV files with US State zipcodes with GPS locations,
find the geographic center of each state with Python and Pandas Dataframes.
See file **blob_process.py**

## Build and list the Docker image locally (see the Dockerfile)

```
docker build -t cjoakim/azure-batch-shipyard1 .

docker image ls | grep azure-batch-shipyard1 
```

Execute/test the Docker image locally; note how environment variables can
be passed into the image with -e:
```
docker run -d -e AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT -e AZURE_STORAGE_KEY=$AZURE_STORAGE_KEY cjoakim/azure-batch-shipyard1b:latest
```

Push the image to DockerHub after testing it locally:
```
docker push cjoakim/azure-batch-shipyard1:latest
```

## Edit the Shipyard configuration files

First, use the Azure CLI to see what VM sizes are available:
```
az vm list-sizes -l eastus > vm-sizes-eastus.json
```

Manually edit these four yaml configuration files:
```
config.yaml
credentials.yaml
jobs.yaml
pool.yaml
```

Or, optionally generate these four yaml files with **shipwright.py**:
```
python shipwright.py --function gen_config_files
python shipwright.py --function parse_config_files
```

See the generated/edited yaml files in the config/ directory.


## Submit the Shipyard configuration files

First, create the **Pool** for your Azure Batch Job(s):
```
shipyard pool add --configdir config --show-config
```

Then, after the pool is created, submit the **Job(s)**:
```
shipyard jobs add --configdir config --show-config
or
shipyard jobs add --configdir config --show-config --tail stdout.txt
or
shipyard jobs add --configdir config --show-config --tail stderr.txt
```

When the job(s) are completed, you can fetch their **stdout** and **stderr** logs
like this:
```
shipyard data files task --configdir config --all --filespec "pyjob201808130525,task-000*,std*.txt"
```

Better yet, your Batch jobs can log to a **remote log system** like Azure Storage,
Azure Service Bus, Azure Event Hubs, Azure CosmosDB, etc.  

See the **batchlog** container where logging JSON files are written to by 
blob_process.py.

See **examples/simple_examples.py**

