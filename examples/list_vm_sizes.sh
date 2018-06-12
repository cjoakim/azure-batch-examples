#!/bin/bash

echo 'Invoking the Azure CLI to get the list of VM sizes...'

az vm list-sizes > vm-sizes.json

echo 'done'
