from census.logger import logging
from census.exception import CensusException
from census.constants import *
from census.entity.config_entity import DataIngestionConfig
from census.entity.artifact_entity import DataIngestionArtifact
from census.config.mongodb_configuration import MongoDBClient
from census.config.spark_manager import spark_session
import os,sys
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



class DataIngestion:

    def __init__(self,data_ingestion_config:DataIngestionConfig,mongodb_config:MongoDBClient):

        self.data_ingestion_config = data_ingestion_config
        self.mongodb_config = mongodb_config

    def download_mongo_data_to_parquet(self):
        try:
            collection = self.mongodb_config.collection
            logging.info("The Mongodb Collection has been succesfully built")
            data =list(collection.find({},{"_id": 0}))
            os.makedirs(self.data_ingestion_config.data_feauture_store,exist_ok=True)
            json_file_path = os.path.join(self.data_ingestion_config.data_feauture_store,DATA_INGESTION_FILENAME)
            # with open(json_file_path, 'w') as f:
            #     f.write(json_data)
            with open(json_file_path, 'w') as f:
                json.dump(data, f)
            logging.info(f"The length of the data is {len(data)}")
            parquet_file_path = os.path.join(self.data_ingestion_config.data_parquet_dir,DATA_INGESTION_PARQUET_FILENAME)
            for file_name in os.listdir(self.data_ingestion_config.data_feauture_store):
                json_file_path = os.path.join(self.data_ingestion_config.data_feauture_store, file_name)
                logging.debug(f"Converting {json_file_path} into parquet format at {parquet_file_path}")
                df = spark_session.read.json(json_file_path)
                if df.count() > 0:
                    df.write.mode('overwrite').parquet(DATA_INGESTION_PARQUET_FILENAME)
            logging.info(f'The parquet file has been stored in {parquet_file_path}')
            return parquet_file_path
            # table = pa.Table.from_pandas(df)
            # pq.write_table(table, parquet_file_name)
        except Exception as e:
            raise CensusException(e,sys)
        

    def initiate_data_ingestion(self):
        try:
            logging.info("Data Ingestion has started")
            os.makedirs(self.data_ingestion_config.data_ingestion_dir,exist_ok=True)
            os.makedirs(self.data_ingestion_config.data_parquet_dir,exist_ok=True)
            self.download_mongo_data_to_parquet()
            logging.info("Data Ingestion has been succefully Completed")
        except Exception as e:
            raise CensusException(e,sys)
        
        

