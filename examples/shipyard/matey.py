"""
Usage:
  python matey.py --function gen_job_env_vars
  python matey.py --function parse_config_files
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# Chris Joakim, Microsoft, 2018/07/30

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

    elif args.function == 'parse_config_files':
        parse_config_files()

    else:
        print('unknown function: {}'.format(args.function))

