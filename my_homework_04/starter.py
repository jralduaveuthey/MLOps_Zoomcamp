#!/usr/bin/env python
# coding: utf-8

import pickle
import pandas as pd
import sys


def read_data(filename,categorical,year=2019,month=6):
    df = pd.read_parquet(filename)
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    return df

def predict(df,categorical,year,month):
    dicts = df[categorical].to_dict(orient='records')
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)
    print(f'Mean predicted duration for {year:04d}-{month:02d} is {y_pred.mean()}')
    return y_pred


def save_result(df, y_pred, output_file):
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['results'] = y_pred
    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )
    print(f'{output_file} saved locally.')


def run():
    year = int(sys.argv[1]) # 2021
    month = int(sys.argv[2]) # 2 
    categorical = ['PUlocationID', 'DOlocationID']
    output_file = f'output/output_{year:04d}-{month:02d}.parquet'
    # output_file = 'output_file_homework'  
    
    df = read_data(f'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{year:04d}-{month:02d}.parquet',categorical,year,month)
    y_pred = predict(df,categorical,year,month)
    save_result(df, y_pred, output_file)
    


if __name__ == '__main__':
    run()
