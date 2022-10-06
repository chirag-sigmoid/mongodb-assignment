import pymongo

try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
except:
    print("Error Occurred During Connection")

db = client["sample_mflix"]


# Top 10 cities with the maximum number of theatres
def top_10_cities_max_theatres():
    output = db.theaters.aggregate([{"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
                                   {"$project": {"location.address.city": 1, "count": 1}},
                                   {"$sort": {"count": -1}},
                                   {"$limit": 10}])
    with open(f'queries_outputfiles/theatre_queries/top_10_cities_with_max_theatres.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_10_cities_max_theatres()


# top 10 theatres nearby given coordinates
def top_10_theatres_nearby_given_coordi():
    output = db.theaters.aggregate([{"$geoNear": {"near": {"type": "Point", "coordinates": [-76.512016, 38.29697]},"maxDistance": 10000000, "distanceField": "distance"}},
                                    {"$project": {"location.address.city": 1, "_id": 0, "location.geo.coordinates": 1}},
                                    {"$limit": 10}])
    with open(f'queries_outputfiles/theatre_queries/top_10_theatres_nearby_given_coordi.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_10_theatres_nearby_given_coordi()
