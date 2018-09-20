import json
import os
import sys

import pandas as pd

# Get the mean latitude/longitude location for the given US state code.
# Chris Joakim, Microsoft, 2018/09/20
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

            # Use Pandas to find the center of the given state csv file
            # containng GPS coordinates for each zip code, for example:
            #
            # id,postal_cd,country_cd,city_name,state_abbrv,latitude,longitude
            # 11450,28031,US,Cornelius,NC,35.4708460000,-80.8862090000
            # 11451,28032,US,Cramerton,NC,35.2343220000,-81.0792760000
            # 11452,28033,US,Crouse,NC,35.4172080000,-81.3365400000
            # 11453,28034,US,Dallas,NC,35.3516260000,-81.1837240000
            # 11454,28035,US,Davidson,NC,35.5095000000,-80.8433000000
            # 11455,28036,US,Davidson,NC,35.4833060000,-80.7978540000

            df = pd.read_csv(infile, delimiter=',')
            describe(df)
            mean_lat = df["latitude"].mean()
            mean_lng = df["longitude"].mean()
            print('result,{},{},{}'.format(st, mean_lat, mean_lng))
            print("")
            print("python script completed")
            print("")
        else:
            print("invalid function: {}".format(func))
    else:
        print("no command-line function given")
