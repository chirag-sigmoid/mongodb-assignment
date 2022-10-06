import pymongo

db = pymongo.MongoClient("localhost", 27017).sample_mflix


def insert(collection, newItem):
    db[collection].insert_one(newItem)


newDoc = {
    "College": "IIT BHU",
    "City": "Varanasi"
}

collection_list = ["comments", "movies", "sessions", "theaters", "users"]
for collectionName in collection_list:
    insert(collectionName, newDoc)
