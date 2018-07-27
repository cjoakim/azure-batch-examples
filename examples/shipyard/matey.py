"""
Usage:
  python matey.py --function gen_job_env_vars
Options:
  -h --help     Show this screen.
  --version     Show version.
"""

# Chris Joakim, Microsoft, 2018/07/27

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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--function', required=True, help='The logical function to execute')
    args = parser.parse_args()

    if args.function == 'gen_job_env_vars':
        gen_job_env_vars()

    else:
        print('unknown function: {}'.format(args.function))

