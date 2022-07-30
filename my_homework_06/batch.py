#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
import os





def read_data(filename, categorical):
    # reading (I/O) 
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
    # NOTE: Setting the endpoint will use localstack as virtual AWS. Removing that argument will use the real AWS.
    if S3_ENDPOINT_URL is None:
        df = pd.read_parquet(filename)
    else:          
        options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
            }
        }   
        df = pd.read_parquet(filename, storage_options=options)
    
    df = prepare_data(df,categorical)
        
    return df

def save_data(df_output,output_file):
    # reading (I/O) 
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
    # NOTE: Setting the endpoint will use localstack as virtual AWS. Removing that argument will SAVE THE FILE LOCALLY (see function get_output_path) BECAUSE I CANNOT 
    # SAVE IT IN THE BUCKET FROM ALEXEY
    if S3_ENDPOINT_URL is None:
        df_output.to_parquet(
            output_file,
            engine='pyarrow',
            index=False,
        )
    else:
        options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
            }
        }   
        df_output.to_parquet(
            output_file,
            engine='pyarrow',
            compression=None,
            index=False,
            storage_options=options
        )
            
    return 

def prepare_data(df,categorical):
    # tranformation
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def get_input_path(year, month):
    default_input_pattern = 'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 'taxi_type=fhv_year={year:04d}_month={month:02d}.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

# Defining main function
def main(year, month):
    categorical = ['PUlocationID', 'DOlocationID']
    input_file = get_input_path(year, month)
    print('I am in batch.py and the input file is : ' + input_file)
    # output_file = f's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet
    output_file = get_output_path(year, month)
    # output_file = f'taxi_type=fhv_year={year:04d}_month={month:02d}.parquet'
    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
        
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted sum duration:', y_pred.sum())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    # df_result.to_parquet(output_file, engine='pyarrow', index=False) #NOTE: this was used till Q6
    save_data(df_result,output_file)
    print('File saved correctly to :'+ output_file)
    
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    
  
  
if __name__=="__main__": 
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)