import pymongo

try:
    connection = pymongo.MongoClient("mongodb://localhost:27017/")
except:
    print("Error Occurred During Connection")

db = connection["sample_mflix"]


# Find top 10 users who made the maximum number of comments
def top_10_user_max_comments():
    output = db.comments.aggregate([{"$group": {"_id": "$name", "comments": {"$sum": 1}}},
                                    {"$sort": {"comments": -1}},
                                    {"$limit": 10}
                                    ])
    with open('queries_outputfiles/comments_queries/top_10_user_max_comments.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_10_user_max_comments()


# Find top 10 movies with most comments
def top_10_movie_most_comments():
    output = db.comments.aggregate([{"$group": {"_id": "$movie_id", "comments": {"$sum": 1}}},
                                    {"$sort": {"comments": -1}},
                                    {"$limit": 10}
                                    ])
    with open('queries_outputfiles/comments_queries/top_10_movie_most_comments.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_10_movie_most_comments()


# Given a year find the total number of comments created each month in that year
def total_comments_given_year(year):
    output = db.comments.aggregate(
        [{"$group": {"_id": {"year": {"$year": "$date"}, "month": {"$month": "$date"}}, "comments": {"$sum": 1}}},
         {"$match": {"_id.year": {"$eq": year}}},
         {"$sort": {"_id.month": 1}}
         ])
    with open(f'queries_outputfiles/comments_queries/total_comments_given_month_{year}.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


total_comments_given_year(1983)
