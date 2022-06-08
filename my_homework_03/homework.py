import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pickle

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner


@task
def read_data(path):
    # logger = get_run_logger()
    # logger.info("INFO reading the data.")
    df = pd.read_parquet(path)
    return df

@task
def prepare_features(df, categorical, train=True):
    # logger = get_run_logger()
    # logger.info("INFO Preparing the features.")
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    if train:
        print(f"The mean duration of training is {mean_duration}")
    else:
        print(f"The mean duration of validation is {mean_duration}")
    
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df

@task
def train_model(df, categorical):
    # logger = get_run_logger()
    # logger.info("INFO training the model.")
    
    train_dicts = df[categorical].to_dict(orient='records')
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts) 
    y_train = df.duration.values

    print(f"The shape of X_train is {X_train.shape}")
    print(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    print(f"The MSE of training is: {mse}")
    return lr, dv

@task
def run_model(df, categorical, dv, lr):
    # logger = get_run_logger()
    # logger.info("INFO running the model.")
    
    val_dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(val_dicts) 
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)
    print(f"The MSE of validation is: {mse}")
    return

@task
def get_paths(date=None):
    if date==None:
        mydatetime = date.today()
    else:
        mydatetime = datetime.strptime(date, '%Y-%m-%d')
    train_datetime = mydatetime - relativedelta(months=1)
    val_datetime = mydatetime - relativedelta(months=2)
    
    if len(str(train_datetime.month))==1:
        train_path = './data/fhv_tripdata_2021-0' + str(train_datetime.month) + '.parquet'
    else: 
        train_path = './data/fhv_tripdata_2021-' + str(train_datetime.month) + '.parquet'
    if len(str(val_datetime.month))==1:
        val_path = './data/fhv_tripdata_2021-0' + str(val_datetime.month) + '.parquet'
    else: 
        val_path = './data/fhv_tripdata_2021-' + str(val_datetime.month) + '.parquet'
    
    return train_path, val_path

@flow(task_runner=SequentialTaskRunner())
# def main(train_path: str = './data/fhv_tripdata_2021-01.parquet', 
#            val_path: str = './data/fhv_tripdata_2021-02.parquet'):
def main(date=None):
    train_path, val_path = get_paths(date).result()

    categorical = ['PUlocationID', 'DOlocationID']

    df_train = read_data(train_path)
    df_train_processed = prepare_features(df_train, categorical)

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, False)

    # train the model
    lr, dv = train_model(df_train_processed, categorical).result()
    run_model(df_val_processed, categorical, dv, lr)
    
    with open('models/model-' + date + '.bin', 'wb') as f_out:
        pickle.dump((lr), f_out)
        print('Model saved as .bin')
    with open('models/dv-' + date + '.b', 'wb') as f_out:
        pickle.dump((dv), f_out)
        print('DictVectorizer  saved as .b')

# main(date="2021-08-15")

from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner

DeploymentSpec(
    flow=main, #NOTE: you do not call it with a parameter, if you want to call with parameters use "parameters" as explained here https://orion-docs.prefect.io/tutorials/deployments/
    name="my-scheduled-deployment",
    # flow_location="/path/to/flow.py",
    schedule=CronSchedule(cron="0 9 15 * *"),
    flow_runner=SubprocessFlowRunner(), #so it works with the local storage (see video min 7:40 for more info python prefect_flow.py)
    tags=["ml"]
)