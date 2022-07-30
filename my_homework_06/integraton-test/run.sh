#!/usr/bin/env bash

cd "$(dirname "$0")" #NOTE: this is so to change the directory to the one where the bash script is



LOCAL_TAG=`date +"%Y-%m-%d-%H-%M"`
export LOCAL_IMAGE_NAME="homework_06:${LOCAL_TAG}"
docker build -t ${LOCAL_IMAGE_NAME} ..

docker-compose -f docker-compose.yaml up -d #NOTE: here we start the s3 bucket service

sleep 1

#NOTE: we need to create a new s3 bucket! >> as soon as we kill the docker all its contents will die with it (including the bucket)
aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration 

#Integration test from Q6
export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
export S3_ENDPOINT_URL="http://localhost:4566"

cd .. #NOTE: I need to go one level up to "homework" so it can find the model.bin
pipenv run python integraton-test/integration_test.py


cd "$(dirname "$0")" #NOTE: I need to change back to this directory to do docker-compose down since I have to do it where the docker-compose.yaml is
docker-compose down
