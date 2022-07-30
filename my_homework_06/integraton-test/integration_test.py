from datetime import datetime
import pandas as pd
import sys
sys.path.insert(1, '/home/ubuntu/mlops-zoomcamp/06-best-practices/homework')
import batch
import os

def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)


########################## Q5 ##########################
data = [
(None, None, dt(1, 2), dt(1, 10)),
(1, 1, dt(1, 2), dt(1, 10)),
(1, 1, dt(1, 2, 0), dt(1, 2, 50)),
(1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
]
    
    
columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
categorical = ['PUlocationID', 'DOlocationID']
df_input = pd.DataFrame(data, columns=columns)

S3_ENDPOINT_URL = 'http://localhost:4566' #NOTE: I have to define it here because this is not an environmental variable being exported in run.sh. 
                                            #It is only defined in docker-compose.yaml which means that only inside the docker is accesible
options = {
'client_kwargs': {
    'endpoint_url': S3_ENDPOINT_URL
    }
}   

input_file = batch.get_input_path(2021, 1) #NOTE: from instructions homework: We will pretent that this is data for January 2021.
#NOTE: the next 2 lines only because I keep getting the error ModuleNotFoundError: No module named 'batch'
# input_pattern ="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
# input_file = input_pattern.format(year=2021, month=1)

print('I am in integration_test.py and the input file is: ' + input_file)
    
df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

print("Q5: Filed imported correctly to S3")


########################## Q6 ##########################
os.system('python /home/ubuntu/mlops-zoomcamp/06-best-practices/homework/batch.py 2021 01') #NOTE: this is correct because I have called it directly from python