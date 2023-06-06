from os import environ
from pymongo.mongo_client import MongoClient
#from pymongo.server_api import ServerApi

class Database:

    def __init__(self, database):
        self.username = environ['DBUSER']
        self.password = environ['DBPASS']
        self.cluster_url = environ['DBURL']
        self.database = database
        self.client = None
        self.db = None

    def connect(self):
        connection_url = f"mongodb+srv://{self.username}:{self.password}@{self.cluster_url}/{self.database}?retryWrites=true&w=majority"
        self.client = MongoClient(connection_url)
        self.db = self.client[self.database]

        #print(self.db)
        
    def disconnect(self):
        if self.client:
            self.client.close()

    def get_collection(self, collection_name):
        if self.db != None:
            return self.db[collection_name]
        else:
            raise Exception('No database connection established.')