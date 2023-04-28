from census.constants import *
import os

class ClientDataPreprocessingConfig:

    def __init__(self):
        self.client_data_artifact_dir = os.path.join(ARTIFACT_DIR,CLIENT_PREPROCESSING_DATA_DIR)


class DataIngestionConfig():

    def __init__(self):
        self.data_ingestion_dir = os.path.join(ARTIFACT_DIR,DATA_INGESTION_ARTIFACT_DIR)
        self.data_feauture_store = os.path.join(self.data_ingestion_dir,DATA_INGESTION_FEATURE_DIR)
        self.data_parquet_dir = os.path.join(self.data_ingestion_dir,DATA_INGESTION_PARQUET_DIR)
        