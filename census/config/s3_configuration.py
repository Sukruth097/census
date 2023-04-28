from census.constants import *
import os
import boto3
from census.logger import logging
from census.exception import CensusException
import sys
from typing import Optional


class s3Config:

    def __init__(self):

        self.client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )

    def download_s3_files(self,bucket_name,prefix:Optional[str],destination_bucket_name,data_file_path):
        try:
            logging.info(f"s3 connection is successful")
            logging.info(f"Accessing s3://{bucket_name}/{prefix}")
            if prefix is not None:
                response = self.client.list_objects_v2(Bucket=bucket_name,Prefix=prefix)
            else:
                response = self.client.list_objects_v2(Bucket=bucket_name)

            if 'Contents' in response:
                objects = response['Contents']
            else:
                objects = f"No objects or files found in the s3://{bucket_name}"
                logging.info(f"No objects or files found in the s3://{bucket_name}/{prefix}")
                
            # keys = [obj['Key'] for obj in objects]
            # logging.info(f"The objects or files found in s3 are {keys}")

            # download each JSON object in the list
            for obj in objects:
                key = obj['Key']
                # print(key)
                filename = os.path.join(data_file_path, key)
                # print(filename)
                logging.info(f"The filename is {filename}")
                # check if the file is a JSON file
                if filename.endswith('.json'):
                    # create the directory if it does not exist
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    # download the file
                    self.client.download_file(bucket_name, key, filename)
                    logging.info(f'{filename} downloaded successfully')

            # return filename

            logging.info(f"Archiving file from data source: s3://{bucket_name}/data/  to archive: s3://{destination_bucket_name}/archive ")
            os.system(f"aws s3 mv s3://{bucket_name}/data/ s3://{destination_bucket_name}/  --recursive --exclude '*' --include '*.json'")
            logging.info(f"Data or files has been moved to {destination_bucket_name} succesfully ")

        except Exception as e:
            CensusException(e,sys)