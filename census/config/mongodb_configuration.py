from census.constants import *
from census.logger import logging
from census.exception import CensusException
import pymongo
import os,sys
import certifi
ca = certifi.where()

class MongoDBClient:
    client = None
    def __init__(self, database_name=DATABASE_NAME,collection_name=COLLECTION_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                #mongo_db_url = os.getenv
                mongo_db_url  = f"mongodb+srv://{os.environ['USER_NAME']}:{os.environ['PASSWORD']}@cluster0.yd8mlom.mongodb.net/?retryWrites=true&w=majority"
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.collection = self.database[collection_name]
            self.database_name = database_name
            self.collection_name = collection_name
        except Exception as e:
            raise CensusException(e,sys)