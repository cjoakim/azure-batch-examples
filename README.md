# azure-batch-examples

Azure Batch compute service examples

# Links

- https://docs.microsoft.com/en-us/azure/batch/
- https://docs.microsoft.com/en-us/azure/batch/quick-run-python
- https://docs.microsoft.com/en-us/azure/batch/batch-api-basics
- https://docs.microsoft.com/en-us/azure/batch/batch-python-tutorial
- https://github.com/Azure/azure-batch-samples
- https://github.com/cjoakim/azure-batch-examples (this repo)
- https://pymotw.com/2/zipfile/


# Azure Batch Diagram

![Diagram](tech_overview_02.png)

---

# Azure Setup

Create the following Azure PaaS Services:

- Storage
- Batch
- CosmosDB with SQL/DocumentDB API

---

# Workstation Requirements and Setup

These instructions assume macOS.

Required Software:
- Git
- Python 3.6

Clone this GitHub repository:
```
cd <your-chosen-directory>
git clone git@github.com:cjoakim/azure-batch-examples.git
cd azure-batch-examples
```

## Set Environment Variables

Set the following per your Azure Batch, Storage, and CosmosDB accounts:

- AZURE_BATCH_ACCOUNT
- AZURE_BATCH_KEY
- AZURE_BATCH_URL
- AZURE_STORAGE_ACCOUNT
- AZURE_STORAGE_ACCESS_KEY
- AZURE_COSMOSDB_DOCDB_URI
- AZURE_COSMOSDB_DOCDB_KEY

The use of environment variables for configuration is highly recommended
vs hard-coding these credentials and other values into your source code.
See the Twelve-Factor App; https://12factor.net

## Create Python Virtual Environment

```
$ cd examples
$ ./venv.sh    
```

venv.sh uses pip and the requirements.txt file to create the python 
virtual environment on your workstation.

---

# The Example Apps

There are currently two example apps in this repo, in the examples/ directory.
- Zip file extraction and loading extracted CSV to CosmosDB
- Determine the mean/center location of given US States given Postal Code data.

---

## Example App 1 

The first example consists of two Azure Batch Jobs.  Each job is submitted from your
workstation with a **client** python application, and runs in Azure Batch
as a python **task**.

Before executing the first job, we need to create the Azure Blob Storage
Containers that will be used in the app.  Execute the following to do this;
it utilizes the Azure CLI.

```
$ ./unzip_job_prepare.sh
```

## The CSV Data

The CSV data in the Zip files looks like the following; it contains US Postal Code
information for each postal code in North Carolina. 

```
id,postal_cd,country_cd,city_name,state_abbrv,latitude,longitude
11029,27229,US,Candor,NC,35.2562750000,-79.7998240000
11030,27230,US,Cedar Falls,NC,35.7518000000,-79.7317000000
11031,27231,US,Cedar Grove,NC,36.1956520000,-79.1699700000
11032,27233,US,Climax,NC,35.8938970000,-79.6998560000
11033,27235,US,Colfax,NC,36.0958730000,-80.0047100000
11034,27237,US,Cumnock,NC,35.5532000000,-79.2348000000
11035,27239,US,Denton,NC,35.6202570000,-80.0850570000
11036,27242,US,Eagle Springs,NC,35.3340640000,-79.6397000000
11037,27243,US,Efland,NC,36.0658920000,-79.1995670000
11038,27244,US,Elon,NC,36.2048000000,-79.4860890000
11039,27247,US,Ether,NC,35.4403000000,-79.7839000000
11040,27248,US,Franklinville,NC,35.7797640000,-79.7083040000
11041,27249,US,Gibsonville,NC,36.1710530000,-79.5856080000
11042,27252,US,Goldston,NC,35.5640480000,-79.3448920000
11043,27253,US,Graham,NC,35.9672710000,-79.3357490000
11044,27256,US,Gulf,NC,35.5666260000,-79.2827790000
11045,27258,US,Haw River,NC,36.0447680000,-79.3233640000
11046,27259,US,Highfalls,NC,35.4793000000,-79.5233000000
11047,27260,US,High Point,NC,35.9565700000,-79.9927520000
11048,27261,US,High Point,NC,35.9557000000,-80.0057000000
```

## The Resulting DocmentDB Data

Documents are inserted into DocumentDB from this CSV data,
each document looks similar to this:

```
{
  "_attachments": "attachments/",
  "_etag": "\"20006902-0000-0000-0000-595912190000\"",
  "_rid": "GxMfAOnHMQHhAQAAAAAAAA==",
  "_self": "dbs/GxMfAA==/colls/GxMfAOnHMQE=/docs/GxMfAOnHMQHhAQAAAAAAAA==/",
  "_ts": 1499009561,
  "city_name": "Candor",
  "country_cd": "US",
  "id": "8da7adc7-38d0-4ef0-8bd4-c4418e16a855",
  "latitude": "35.2562750000",
  "longitude": "-79.7998240000",
  "pkey": "Candor",
  "postal_cd": "27229",
  "seq": "11029",
  "state_abbrv": "NC"
}
```

## Executing the First Azure Batch Job

The first job uploads the three data/NC*.zip files, containing csv files, 
to the "batchzips" container Azure Blob Storage, then unzips them into the
"batchcsv" container in Azure Blob storage.

The client python program first uploads the "task" python script to the "batchtask"
container in Blob Storage.  Azure Batch then runs that task script on the dynamic
VMs that are allocated for the batch job.

The second job also uses a python client, and python task in Azure Batch.
The task script reads and parses the csv files in Azure Blob Storage, and loads 
them into the Azure DocumentDB "zipdata" collection within the "dev" database.


The first job is executed as follows from a Bash Terminal:
```
$ ./unzip_job.sh
```

The key line in this job is the following, which executes the Python Azure Batch
client program:

```
$ python unzip_client.py --pool UnzipPool --job unzip --task unzip_task.py
```

## Executing the Second Azure Batch Job

The second job is executed as follows from a Bash Terminal:
```
$ ./csv_etl_job.sh
```

The key line in this job is the following, which executes the Python Azure Batch
client program:

```
$ python csv_etl_client.py --pool CsvEtlPool --job csvetl --task csv_etl_task.py --nodecount 5
```

## Querying DocumentDB

You can query the resulting documents that are in DocumentDB by running this Python script:

```
$ source bin/activate
$ python docdb.py --func query_all_zipdata_docs
```

---

## Example App 2


---

# Comments on the Python code used

I rewrote the Azure Batch sample code found here: https://github.com/Azure/azure-batch-samples
into a more Object-Oriented form.  This OO form is implemented in file **examples/batch_client.py**; I believe
that class **BatchClient**, found in batch_client.py, enables better code reuse.

You are free to modify this code as necessary.  This code should not be considered "production quality"
as it is intended for demonstration purposes only.
