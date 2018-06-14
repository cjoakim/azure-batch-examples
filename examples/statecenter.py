import json
import os
import sys

import pandas as pd

# Get the mean latitude/longitude location for the given US state code.
# Chris Joakim, Microsoft, 2018/06/14
# export st=fl ; python statecenter.py state_center

def describe(df):
    print("describe the pandas DataFrame")
    print('=== type(df)')
    print(type(df))  # <class 'pandas.core.frame.DataFrame'>
    print('=== df.columns')
    print(df.columns)
    print('=== df.head()')
    print(df.head())
    print('=== df.tail(1)')
    print(df.tail(1))
    print('=== df.index')
    print(df.index)
    print('=== df.dtypes')
    print(df.dtypes)
    print('=== df.describe()')
    print(df.describe())
    print("")
    print("")

if __name__ == "__main__":

    if len(sys.argv) > 1:
        func = sys.argv[1].lower()

        if func == 'state_center':
            st = os.environ["st"].lower()
            infile = 'data/postal_codes_{}.csv'.format(st)
            print('st, infile: {} -> {}'.format(st, infile))
            df = pd.read_csv(infile, delimiter=',')
            describe(df)

            mean_lat = df["latitude"].mean()
            mean_lng = df["longitude"].mean()
            csv_line = '{},{},{}'.format(st, mean_lat, mean_lng)
            print(csv_line)
            print("")
        else:
            print("invalid functio: {}".format(func))
    else:
        print("no command-line function given")