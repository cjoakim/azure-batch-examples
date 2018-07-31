"""
Usage:
  python launch.py --function gen_config_files
  python launch.py --function gen_job_env_vars
  python launch.py --function parse_config_files
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# Chris Joakim, Microsoft, 2018/07/31

from __future__ import print_function
import argparse
import collections
import csv
import datetime
import json
import os
import string
import sys
import time
import yaml


def launch_config():
    # TODO - read this dict from a json file
    c = dict()
    c['docker_image'] = 'cjoakim/azure-batch-shipyard1:latest'
    c['storage_acct'] = 'cjoakimstdstorage'
    c['storage_key']  = find_env_var_key(c['storage_acct'])
    print(json.dumps(c, sort_keys=True, indent=2))
    return c

def find_env_var_key(name_value):
    # by convention, environment variables in this pattern are expected:
    # AZURE_XXX_ACCOUNT & AZURE_XXX_KEY
    for n in sorted(os.environ):
        if n.startswith('AZURE'):
            v = os.getenv(n)
            if v == name_value:
                tokens = n.split('_')
                tokens[-1] = 'KEY'
                key = '_'.join(tokens)
                return os.getenv(key)
    return '?'

def gen_config_files():
    print('gen_config_files')
    c = launch_config()
    gen_conf_config_yaml(c)
    gen_credentials_config_yaml(c)
    gen_jobs_config_yaml(c)
    gen_pool_config_yaml(c)

def gen_conf_config_yaml(c):
    data = dict()
    batch_dict = dict()
    batch_dict['storage_account_settings'] = c['storage_acct']
    data['batch_shipyard'] = batch_dict
    globals = dict()
    globals['docker_images'] = [c['docker_image']]
    data['global_resources'] = globals
    write_yaml(data, 'config')

def gen_credentials_config_yaml(c):
    data = dict()
    cred_dict = dict()
    batch_dict = dict()
    batch_dict['account_key'] = os.getenv('AZURE_BATCH_KEY')
    batch_dict['account_service_url'] = os.getenv('AZURE_BATCH_URL')
    stor_dict, acct_dict, stor_acct = dict(), dict(), c['storage_acct']
    acct_dict['account'] = stor_acct
    acct_dict['account_key'] = c['storage_key']
    acct_dict['endpoint'] = 'core.windows.net'
    stor_dict[stor_acct] = acct_dict
    cred_dict['batch'] = batch_dict
    cred_dict['storage'] = stor_dict
    data['credentials'] = cred_dict
    write_yaml(data, 'credentials')

    # credentials:
    #   batch:
    #     account_key: <secret>
    #     account_service_url: https://cjoakimbatch.eastus.batch.azure.com
    #   storage:
    #     cjoakimstdstorage:
    #       account: cjoakimstdstorage
    #       account_key: <secret>
    #       endpoint: core.windows.net

def gen_jobs_config_yaml(c):
    data = dict()
    write_yaml(data, 'jobs')

    # job_specifications:
    # - id: cjoakimshipyard4
    #   environment_variables:
    #     AZURE_STORAGE_ACCOUNT: cjoakimstdstorage
    #     AZURE_STORAGE_KEY: 7SPUdEMEMV185ZY/JDlCvybizujNLGoGqR9U5sKYcO3RTS60lo9lEvXdgk7U0DeILYlTQdk/6bBHsT6WM5E4Fw==
    #   tasks:
    #   - docker_image: cjoakim/azure-batch-shipyard1:latest
    #     remove_container_after_exit: true
    #     command: env | sort ; docker run -d -e AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT -e AZURE_STORAGE_KEY=$AZURE_STORAGE_KEY cjoakim/azure-batch-shipyard1:latest

def gen_pool_config_yaml(c):
    data = dict()
    write_yaml(data, 'pool')

    # pool_specification:
    #   id: cjoakimpool30
    #   vm_size: Standard_A5
    #   vm_count:
    #     dedicated: 1
    #     low_priority: 0
    #   vm_configuration:
    #     platform_image:
    #       publisher: Canonical
    #       offer: UbuntuServer
    #       sku: 16.04-LTS
    #   reboot_on_start_task_failed: false
    #   block_until_all_global_resources_loaded: true

def write_yaml(obj, basename):
    outfile = 'config/{}_gen.yaml'.format(basename)
    with open(outfile, 'w') as out:
        yaml.dump(obj, out, default_flow_style=False)
        print('file written: {}'.format(outfile))

def gen_job_env_vars():
    print('  environment_variables:')
    for n in sorted(os.environ):
        if n.startswith('AZURE'):
            v = os.getenv(n)
            print('    {}: {}'.format(n, v))

def parse_config_files():
    print('parse_config_files')

    config_basenames = 'config credentials jobs pool'.split()
    print(config_basenames)  # ['config', 'credentials', 'jobs', 'pool']

    for basename in config_basenames:
        infile = 'config/{}.yaml'.format(basename)
        print('===')
        print('reading/parsing file: {}'.format(infile))
        with open(infile, 'r') as stream:
            try:
                obj = yaml.load(stream)
                print(json.dumps(obj, sort_keys=True, indent=2))
            except yaml.YAMLError as exc:
                print(exc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--function', required=True, help='The logical function to execute')
    args = parser.parse_args()

    if args.function == 'gen_job_env_vars':
        gen_job_env_vars()

    elif args.function == 'gen_config_files':
        gen_config_files()

    elif args.function == 'parse_config_files':
        parse_config_files()

    else:
        print('unknown function: {}'.format(args.function))

