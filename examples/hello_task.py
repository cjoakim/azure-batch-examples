from __future__ import print_function
import argparse
import collections
import datetime
import json
import os
import string
import time
import zipfile

import azure.storage.blob as azureblob

# Azure Batch Task which will be executed on the Azure Batch nodes.
# Chris Joakim, Microsoft, 2018/09/13


if __name__ == '__main__':
    print('Batch Task {} at {}'.format(__file__, datetime.datetime.utcnow()))
    parser = argparse.ArgumentParser()
    args = parser.parse_args()