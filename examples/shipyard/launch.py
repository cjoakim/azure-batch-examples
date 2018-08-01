"""
Usage:
  python launch.py --function gen_config_files
  python launch.py --function parse_config_files
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# The purpose of this script is to both generate and parse/validate
# Shipyard configuration files.  It is a working proof-of-concept
# for a more complete implementation.  For config file generation,
# edit the 'launch.json' config file with your specific values.
# Chris Joakim, Microsoft, 2018/07/31

from __future__ import print_function
import argparse
import arrow
import collections
import csv
import datetime
import json
import os
import string
import sys
import time
import yaml


def gen_config_files():
    print('gen_config_files')
    c = launch_config()
    gen_config_config_yaml(c)
    gen_credentials_config_yaml(c)
    gen_jobs_config_yaml(c)
    gen_pool_config_yaml(c)
    print('Shipyard config files generated per the following values:')
    print(json.dumps(c, sort_keys=True, indent=2))

def gen_config_config_yaml(c):
    template_obj = load_yaml_file('config_examples/config.yaml')
    template_obj['batch_shipyard']['storage_account_settings'] = c['storage_acct']
    template_obj['global_resources']['docker_images'][0] = c['docker_image']
    write_config_yaml(template_obj, 'config')

def gen_credentials_config_yaml(c):
    template_obj = load_yaml_file('config_examples/credentials.yaml')
    template_obj['credentials']['batch']['account_key'] = os.getenv('AZURE_BATCH_KEY')
    template_obj['credentials']['batch']['account_service_url'] = os.getenv('AZURE_BATCH_URL')
    stor_dict, acct_dict, stor_acct = dict(), dict(), c['storage_acct']
    acct_dict['account'] = stor_acct
    acct_dict['account_key'] = c['storage_key']
    acct_dict['endpoint'] = 'core.windows.net'
    stor_dict[stor_acct] = acct_dict
    template_obj['credentials']['storage'] = stor_dict
    write_config_yaml(template_obj, 'credentials')

def gen_jobs_config_yaml(c):
    job_name = '{}{}'.format(c['job_basename'], c['date_time'])
    template_obj = load_yaml_file('config_examples/jobs.yaml')
    template_obj['job_specifications'][0]['id'] = job_name
    env_vars = dict()
    for env_var in c['job_env_var_names']:
        env_vars[env_var] = os.getenv(env_var)
    template_obj['job_specifications'][0]['environment_variables'] = env_vars
    template_obj['job_specifications'][0]['tasks'][0]['docker_image'] = c['docker_image']
    template_obj['job_specifications'][0]['tasks'][0]['command'] = c['command']
    write_config_yaml(template_obj, 'jobs')
    c['job_env_var_values'] = env_vars
    c['job_name'] = job_name

def gen_pool_config_yaml(c):
    template_obj = load_yaml_file('config_examples/pool.yaml')
    template_obj['pool_specification']['id'] = c['pool_id']
    template_obj['pool_specification']['vm_size'] = c['vm_size']
    template_obj['pool_specification']['vm_count']['dedicated'] = c['vm_count']
    write_config_yaml(template_obj, 'pool')

def launch_config():
    c = read_json_file('launch.json')
    c['epoch'] = arrow.utcnow().timestamp
    c['date_time'] = date_time()
    c['storage_key']  = find_env_var_key(c['storage_acct'])
    c['pool_id'] = pool_id(c)
    return c

def pool_id(c):
    if 'pool_id' in c:
        return c['pool_id']
    else:
        return 'cjoakimpool{}'.format(c['date_time'])

def date_time():
    return arrow.now().format('YYYYMMDDHHmm')

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

def write_config_yaml(obj, basename):
    outfile = 'config/{}.yaml'.format(basename)
    with open(outfile, 'w') as out:
        yaml.dump(obj, out, default_flow_style=False)
        print('file written: {}'.format(outfile))

def read_json_file(infile):
    return json.load(open(infile))

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

def load_yaml_file(infile):
    with open(infile, 'r') as stream:
        try:
            return yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--function', required=True, help='The logical function to execute')
    args = parser.parse_args()

    if args.function == 'gen_config_files':
        gen_config_files()

    elif args.function == 'parse_config_files':
        parse_config_files()

    else:
        print('unknown function: {}'.format(args.function))
