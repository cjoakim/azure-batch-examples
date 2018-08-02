#!/bin/bash

# The following two environment variables are used implicitly:
# AZURE_STORAGE_ACCOUNT
# AZURE_STORAGE_ACCESS_KEY

cname=batchin

az storage container create --name $cname

az storage blob upload --container-name $cname --file data/postal_codes_ct.csv --name postal_codes_ct.csv
az storage blob upload --container-name $cname --file data/postal_codes_fl.csv --name postal_codes_fl.csv
az storage blob upload --container-name $cname --file data/postal_codes_ga.csv --name postal_codes_ga.csv
az storage blob upload --container-name $cname --file data/postal_codes_md.csv --name postal_codes_md.csv
az storage blob upload --container-name $cname --file data/postal_codes_nc.csv --name postal_codes_nc.csv
az storage blob upload --container-name $cname --file data/postal_codes_sc.csv --name postal_codes_sc.csv
az storage blob upload --container-name $cname --file data/postal_codes_va.csv --name postal_codes_va.csv

az storage blob list --container-name $cname

echo 'done'
