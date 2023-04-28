from census.constants import *
from census.entity.config_entity import ClientDataPreprocessingConfig
from census.config.s3_configuration import s3Config
from census.config.mongodb_configuration import MongoDBClient
from census.logger import logging
from census.exception import CensusException
import json
import os,sys
import shutil
import pandas as pd



class ClientDataPreprocess:

    def __init__(self,client_data_config:ClientDataPreprocessingConfig,s3_data_config:s3Config,mongodb_config:MongoDBClient):
        self.client_data_config = client_data_config
        self.s3_data_config = s3_data_config
        self.mongodb_config = mongodb_config

    def preprocess_client_data_to_db(self):
        try:
            self.s3_data_config.download_s3_files(
                bucket_name=BUCKET_NAME,
                prefix=S3_DATA_FOLDER_NAME,
                destination_bucket_name=DESTINATION_BUCKET_NAME,
                data_file_path=self.client_data_config.client_data_artifact_dir
            )
            
            # Create a directory for the new folder
            # new_folder = ARCHIVE_FILES_DIR
            # if not os.path.exists(new_folder):
            #     os.makedirs(new_folder,exist_ok=True)
          

            file_dir = os.path.join(self.client_data_config.client_data_artifact_dir,'data')
            logging.info(f"New data path is {file_dir}")
            print(file_dir)
            if len(os.listdir(file_dir))>1:
                new_df = []
                for file in os.listdir(file_dir):
                    file = os.path.join(file_dir,file)
                    logging.info(f"Reading the file {file}")
                    df = pd.read_json(file)
                    # append the dataframe to the list
                    new_df.append(df)
                # concatenate all dataframes into a single dataframe
                df = pd.concat(new_df)
                logging.info(f" After Concatinating the file, the shape of the dataset was found to be {df.shape}")
                # print(df.shape)
            else:
                # read the single JSON file into a pandas dataframe
                file=os.listdir(file_dir)
                # print(file)
                file_path = os.path.join(file_dir, file[0])             

                # Check if the collection is empty
                logging.info(f"{self.mongodb_config.collection.count_documents({})}")
                if self.mongodb_config.collection.count_documents({}) == 0:
                    df = pd.read_json(file_path)
                    # print(df.shape)
                    logging.info(f"The shape of the data is {df.shape}")

                    # Convert the DataFrame to a list of dictionaries
                    documents = df.to_dict('records')
                    # If the collection is empty, insert the documents into the collection using insert_many()
                    result = self.mongodb_config.collection.insert_many(documents)

                    # Print the number of documents inserted
                    print(f"{len(result.inserted_ids)} documents inserted.")
                else:
                    print("The collection is not empty.")
                    df = pd.read_json(file_path)
                    df_new= df[['complaint_id']]
                    print(df_new.columns)

                    projection = {PRIMARY_KEY: 1, '_id': 0}

                    # query the collection and retrieve only the complaint_id field
                    result = self.mongodb_config.collection.find({}, projection)

                    # convert the result to a pandas DataFrame
                    df_old = pd.DataFrame(list(result))
                    df_old = df_old.rename(columns={PRIMARY_KEY:MODIFIED_PRIMARY_KEY})
                    # df_old['old_complaint_id'].nunique()

                    merged_df = pd.merge(df_new, df_old, left_on=PRIMARY_KEY, right_on=MODIFIED_PRIMARY_KEY,how='left')
                    # merged_df.columns

                    new_records=merged_df[merged_df[PRIMARY_KEY].notna() & merged_df[MODIFIED_PRIMARY_KEY].isna()]
                    
                    #new_records
                    all_records=df[df[PRIMARY_KEY].isin(new_records[PRIMARY_KEY])]

                    # insert the new records into the collection
                    records = all_records.to_dict('records')
                    result=self.mongodb_config.collection.insert_many(records)
                    # Print the number of documents inserted
                    logging.info(f"{len(result.inserted_ids)} documents inserted successfully to MongoDB.")
                    # logging.info("New records inserted successfully to MongoDB")
                    # logging.info(f"Archiving file from data source: s3://{BUCKET_NAME}/data/  to archive: s3://{DESTINATION_BUCKET_NAME}/archive ")
                    # os.system(f"aws s3 mv s3://{BUCKET_NAME}/data/ s3://{DESTINATION_BUCKET_NAME}/  --recursive --exclude '*' --include '*.json'")
                    # logging.info(f"Data or files has been moved to {DESTINATION_BUCKET_NAME} succesfully ")
        except Exception as e:
            CensusException(e,sys)


    def initiate_client_preprocess_data(self):
        try:
            os.makedirs(self.client_data_config.client_data_artifact_dir,exist_ok=True)
            logging.info(f"Created a {os.path.basename(self.client_data_config.client_data_artifact_dir)} folder.")
            self.preprocess_client_data_to_db()
            log_message =f"Data has been successfully dumped to {COLLECTION_NAME}"
            logging.info(f"The log mesaage is {log_message}")
            return log_message
        except Exception as e:
            CensusException(e,sys)

