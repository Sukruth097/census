from census.components.client_data_preprocessing import ClientDataPreprocess
from census.components.data_ingestion import DataIngestion
from census.config.mongodb_configuration import MongoDBClient
from census.logger import logging
from census.exception import CensusException
from census.entity.config_entity import *
import os,sys
from census.config.s3_configuration import s3Config


class TrainPipeline:

    def __init__(self):
        self.client_data_preprocess = ClientDataPreprocessingConfig()
        self.s3_operations = s3Config()
        self.mongodb_config = MongoDBClient()
        self.data_ingestion_config = DataIngestionConfig()

    def start_client_data_preprocess(self):
        try:
            client_data = ClientDataPreprocess(
                client_data_config=self.client_data_preprocess,
                s3_data_config=self.s3_operations,
                mongodb_config=self.mongodb_config
                )
            client_data_artifact = client_data.initiate_client_preprocess_data()
        except Exception as e:
            CensusException(e,sys)

    def start_data_ingestion(self):
        try:
            data_ingestion = DataIngestion(
                data_ingestion_config=self.data_ingestion_config,
                mongodb_config=self.mongodb_config
            )
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        except Exception as e:
            CensusException(e,sys)


    def run_pipeline(self):
        try:
            # client_data_artifact = self.start_client_data_preprocess()
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            CensusException(e,sys)


if __name__ == "__main__":
    train_pipeline =TrainPipeline()
    train_pipeline.run_pipeline()




