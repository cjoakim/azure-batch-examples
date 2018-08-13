# Azure Batch with Shipyard

Azure Batch compute service examples with Shipyard - containerized workloads

# Links

- https://docs.microsoft.com/en-us/azure/batch/batch-technical-overview
- https://azure.github.io/batch-shipyard/
- https://github.com/Azure/batch-shipyard
- https://batch-shipyard.readthedocs.io/en/latest/00-introduction/
- https://azure.microsoft.com/en-us/services/container-registry/
- https://azure.microsoft.com/en-us/services/container-instances/
- https://azure.microsoft.com/en-us/services/kubernetes-service/
- https://azure.microsoft.com/en-us/services/app-service/containers/
- https://docs.microsoft.com/en-us/azure/virtual-machines/windows/sizes-general
- https://github.com/cjoakim/azure-batch-examples (this repo)

---

# What is Azure Batch?

- Azure Batch Compute PaaS service - "Serverless Batch"
- Specify your OS, VM size, number of VMs - 1 to hundreds
- Compute exists only for the duration of of your submitted batch jobs; low cost
- Excellent for **Parallel and HPC** workloads
- Has a **Client SDKs** in multiple programming languages (Python, Java, Node.js, .Net, HTTP)

## Alternatives?

- Data Science Virtual Machine (DSVM)
- Azure Data Lake Analytics (ADLA)
- Azure Databricks 

## Related and Container Services

- Azure Storage - Blobs
- Azure Container Registry (ACR)
- Azure Container Instances (ACI)
- Azure Kubernetes Service (AKS)
- Azure Web Apps with Containers

--- 

# Why use Docker and images?

- Standard and reliable packaging and deployment format
- Build and test locally, deploy anywhere.  WYSIWYG -> WYCIWYG (C = Containerize)
- Excellent foundation for microservices; not monoliths
- More efficient utilization of compute resources - containers vs virtual machines
- Enables orchestration software to manage them at runtime - Azure Batch, Kubernetes

---

# Why use Shipyard?

- Containerize your tasks in Docker images
- Test the images locally on your workstation
- Execute the Docker images in Azure Batch
- No configuration differences - what runs in Batch Shipyard is **exactly** your Docker image
- **Shipyard interacts with the Azure Batch PaaS service via simpler yaml config files**
- **Run anything** in your Docker images in Batch - bash, Python, Java, R, etc
- Shipyard is **open-source**, MIT License. https://github.com/Azure/batch-shipyard
- Runtime environment has well-defined environment variables, for example:

```
"AZ_BATCH_ACCOUNT_NAME": "cjoakimbatch",
"AZ_BATCH_ACCOUNT_URL": "https://cjoakimbatch.eastus.batch.azure.com/",
"AZ_BATCH_CERTIFICATES_DIR": "/mnt/batch/tasks/workitems/pyjob201808131205/job-1/task-00004/certs",
"AZ_BATCH_JOB_ID": "pyjob201808131205",
"AZ_BATCH_JOB_PREP_DIR": "/mnt/batch/tasks/workitems/pyjob201808131205/job-1/jobpreparation",
"AZ_BATCH_JOB_PREP_WORKING_DIR": "/mnt/batch/tasks/workitems/pyjob201808131205/job-1/jobpreparation/wd",
"AZ_BATCH_NODE_ID": "tvm-587366007_1-20180813t153055z",
"AZ_BATCH_NODE_IS_DEDICATED": "true",
"AZ_BATCH_NODE_ROOT_DIR": "/mnt/batch/tasks",
"AZ_BATCH_NODE_SHARED_DIR": "/mnt/batch/tasks/shared",
"AZ_BATCH_NODE_STARTUP_DIR": "/mnt/batch/tasks/startup",
"AZ_BATCH_POOL_ID": "cjoakimpool0813",
"AZ_BATCH_TASK_DIR": "/mnt/batch/tasks/workitems/pyjob201808131205/job-1/task-00004",
"AZ_BATCH_TASK_ID": "task-00004",
"AZ_BATCH_TASK_USER": "_azbatch",
"AZ_BATCH_TASK_USER_IDENTITY": "PoolAdmin",
"AZ_BATCH_TASK_WORKING_DIR": "/mnt/batch/tasks/workitems/pyjob201808131205/job-1/task-00004/wd",
```

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

**Standard_DS3_v2** VM for example; 4CPU, 14GB memory
```
  {
    "maxDataDiskCount": 16,
    "memoryInMb": 14336,
    "name": "Standard_DS3_v2",
    "numberOfCores": 4,
    "osDiskSizeInMb": 1047552,
    "resourceDiskSizeInMb": 28672
  },
```

Manually edit these four yaml configuration files:
```
config.yaml
credentials.yaml
jobs.yaml
pool.yaml
```

Or, optionally generate these four yaml files with **shipwright.py** from
configuration information in **shipwright.json**.
```
python shipwright.py --function gen_config_files
```

See the generated/edited yaml files in the config/ directory.

pool.yaml:
```
pool_specification:
  block_until_all_global_resources_loaded: true
  id: cjoakimpool0813
  reboot_on_start_task_failed: true
  vm_configuration:
    platform_image:
      offer: UbuntuServer
      publisher: Canonical
      sku: 16.04-LTS
  vm_count:
    dedicated: 1
    low_priority: 0
  vm_size: Standard_DS3_v2
```

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

