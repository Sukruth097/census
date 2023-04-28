import os
from datetime import datetime

BUCKET_NAME='census-project-files'
ORIGINAL_BUCKET_NAME ='census-project-files'
DESTINATION_BUCKET_NAME='archive-census'
S3_DATA_FOLDER_NAME='data/'
ARCHIVE_FILES_DIR ='archive_data'
TIMESTAMP:str = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")

ARTIFACT_DIR = os.path.join("artifacts",TIMESTAMP)
CLIENT_PREPROCESSING_DATA_DIR = 'client_data'
MONGODB_URL = f"mongodb+srv://{os.environ['USER_NAME']}:{os.environ['PASSWORD']}@cluster0.yd8mlom.mongodb.net/?retryWrites=true&w=majority"
DATABASE_NAME='census'
COLLECTION_NAME='finance_complaint_data'
PRIMARY_KEY='complaint_id'
MODIFIED_PRIMARY_KEY='old_complaint_id'
CLIENT_DATA_DIR ='client_data'
AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS']


### Data Ingestion Constants
DATA_INGESTION_ARTIFACT_DIR = 'DataIngestion'
DATA_INGESTION_FEATURE_DIR = 'DataFeatureStore'
DATA_INGESTION_FILENAME = 'DataIngested.json'
DATA_INGESTION_PARQUET_DIR = 'DataParquetStore'
DATA_INGESTION_PARQUET_FILENAME= 'DataIngested.parquet'






