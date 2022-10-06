import pymongo
import os


connection = pymongo.MongoClient("localhost", 27017)
print("Connection is successfully made.")

collectionList = ["comments","movies","sessions","theaters","users"]

for collection in collectionList:
        os.system(f"mongoimport --db sample_mflix --collection {collection} --file venv/sample_mflix/{collection}.json")


print(connection.list_database_names())
print(connection["sample_mflix"].list_collection_names())