import pymongo

try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
except:
    print("Error Occurred During Connection")

db = client["sample_mflix"]


# Find top `N` movies with the highest IMDB rating
def top_n_movies_imdb_rating(n):
    output = db.movies.aggregate([{"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
                                {"$match": {"imdb.rating": {"$exists": "true", "$ne": ''}}},
                                {"$sort": {"imdb.rating": -1}},
                                {"$limit": n}
                                ])
    with open(f'queries_outputfiles/movies_queries/top_{n}_movies_imdb_rating.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_movies_imdb_rating(7)

# Find top `N` movies with the highest IMDB rating in a given year
def top_n_highest_imdb_given_year(n, year):
    output = db.movies.aggregate([{"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "year": 1}},
                                {"$match": {"imdb.rating": {"$exists": "true", "$ne": ''}, "year": {"$eq": year}}},
                                {"$sort": {"imdb.rating": -1}},
                                {"$limit": n}
                                ])
    with open(f'queries_outputfiles/movies_queries/top_{n}_highest_imdb_given_year_{year}.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_highest_imdb_given_year(7, 1980)


# Find top `N` movies with highest IMDB rating with number of votes > 1000
def top_n_highest_imdb_votes_greater_than_1000(n):
    output = db.movies.aggregate([{"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "imdb.votes": 1}},
                                {"$match": {"imdb.rating": {"$exists": "true", "$ne": ''},
                                            "imdb.votes": {"$exists": "true", "$gt": 1000}}},
                                {"$sort": {"imdb.rating": -1}},
                                {"$limit": n}
                                ])
    with open(f'queries_outputfiles/movies_queries/top_{n}_highest_imdb_votes_greater_than_1000.txt',
              'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_highest_imdb_votes_greater_than_1000(7)


# Find top `N` movies with title matching a given pattern sorted by highest tomatoes ratings
def top_n_tomatoes_rating_with_matching_string(n, mystr):
    output = db.movies.aggregate([{"$project": {"_id": 0, "title": 1, "tomatoes.viewer.rating": 1}},
                                {"$match": {"title": {"$regex": mystr}}},
                                {"$sort": {"tomatoes.viewer.rating": -1}},
                                {"$limit": n}
                                ])
    with open(
            f'queries_outputfiles/movies_queries/top_{n}_tomatoes_rating_with_matching_string_{mystr}.txt',
            'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_tomatoes_rating_with_matching_string(10, "A ")


# Find top `N` directors who created the maximum number of movies
def top_n_directors_with_max_movies(n):
    output = db.movies.aggregate([{"$unwind": "$directors"},
                                {"$group": {"_id": {"director_name": "$directors"}, "No_of_movies": {"$sum": 1}}},
                                {"$project": {"director_name": 1, "No_of_movies": 1}},
                                {"$sort": {"No_of_movies": -1}},
                                {"$limit": n}
                                ])
    with open(f'queries_outputfiles/movies_queries/top_{n}_directors_with_max_movies.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_directors_with_max_movies(10)

# Find top `N` directors who created the maximum number of movies in a given year
def top_n_directors_with_max_movies_in_given_year(n, year):
    output = db.movies.aggregate([{"$unwind": "$directors"},
                                {"$match": {"year": year}},
                                {"$group": {"_id": {"director_name": "$directors"}, "No_of_movies": {"$sum": 1}}},
                                {"$project": {"director_name": 1, "No_of_movies": 1, "year": 1}},
                                {"$sort": {"No_of_movies": -1}},
                                {"$limit": n}
                                ])
    with open(
            f'queries_outputfiles/movies_queries/top_{n}_directors_with_max_movies_in_given_year_{year}.txt',
            'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_directors_with_max_movies_in_given_year(10, 1980)


# Find top `N` directors who created the maximum number of movies for a given genre
def top_n_directors_with_max_movies_in_given_genre(n, genre):
    output = db.movies.aggregate([{"$unwind": "$directors"},
                                {"$match": {"genres": {"$eq": genre}}},
                                {"$group": {"_id": {"director_name": "$directors"}, "No_of_movies": {"$sum": 1}}},
                                {"$project": {"director_name": 1, "No_of_movies": 1}},
                                {"$sort": {"No_of_movies": -1}},
                                {"$limit": n}
                                ])
    with open(
            f'queries_outputfiles/movies_queries/top_{n}_directors_with_max_movies_in_given_genre_{genre}.txt',
            'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_directors_with_max_movies_in_given_genre(10, "Fantasy")


# Find top `N` actors who starred in the maximum number of movies
def top_n_actors_with_max_movies(n):
    output = db.movies.aggregate([{"$unwind": "$cast"},
                                {"$group": {"_id": {"actor_name": "$cast"}, "No_of_movies": {"$sum": 1}}},
                                {"$project": {"actor_name": 1, "No_of_movies": 1}},
                                {"$sort": {"No_of_movies": -1}},
                                {"$limit": n}
                                ])
    with open(f'queries_outputfiles/movies_queries/top_{n}_actors_with_max_movies.txt', 'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_actors_with_max_movies(10)


# Find top `N` actors who starred in the maximum number of movies in a given year
def top_n_actors_with_max_movies_in_given_year(n, year):
    output = db.movies.aggregate([{"$unwind": "$cast"},
                                {"$match": {"year": year}},
                                {"$group": {"_id": {"actor_name": "$cast"}, "No_of_movies": {"$sum": 1}}},
                                {"$project": {"actor_name": 1, "No_of_movies": 1, "year": 1}},
                                {"$sort": {"No_of_movies": -1}},
                                {"$limit": n}
                                ])
    with open(
            f'queries_outputfiles/movies_queries/top_{n}_actors_with_max_movies_in_given_year_{year}.txt',
            'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_actors_with_max_movies_in_given_year(10, 1980)


# Find top `N` actors who starred in the maximum number of movies for a given genre
def top_n_actors_with_max_movies_in_given_genre(n, genre):
    output = db.movies.aggregate([{"$unwind": "$cast"},
                                {"$match": {"genres": {"$eq": genre}}},
                                {"$group": {"_id": {"actor_name": "$cast"}, "No_of_movies": {"$sum": 1}}},
                                {"$project": {"actor_name": 1, "No_of_movies": 1}},
                                {"$sort": {"No_of_movies": -1}},
                                {"$limit": n}
                                ])
    with open(
            f'queries_outputfiles/movies_queries/top_{n}_actors_with_max_movies_in_given_genre_{genre}.txt',
            'w') as f:
        for line in output:
            f.write(str(line))
            f.write('\n')


top_n_actors_with_max_movies_in_given_genre(10, "Crime")
