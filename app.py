from flask import Flask, request, jsonify
from flask_restful import Api, Resource
from pymongo import MongoClient
from bson import json_util
import json
import os

app = Flask(__name__)
api = Api(app)

# Get MongoDB connection details from environment variables
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'potato_db')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'tweets')

def connect_to_mongodb():
    client = MongoClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    return db[COLLECTION_NAME]

# 1. Tweets per day
class TweetsPerDay(Resource):
    def get(self):
        term = request.args.get('term', '')
        collection = connect_to_mongodb()
        pipeline = [
            {"$match": {"text": {"$regex": term, "$options": "i"}}},
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        result = list(collection.aggregate(pipeline))
        return json.loads(json_util.dumps(result))

# 2. Unique users count
class UniqueUsers(Resource):
    def get(self):
        term = request.args.get('term', '')
        collection = connect_to_mongodb()
        pipeline = [
            {"$match": {"text": {"$regex": term, "$options": "i"}}},
            {"$group": {"_id": "$author_id"}},
            {"$count": "unique_users"}
        ]
        result = list(collection.aggregate(pipeline))
        return json.loads(json_util.dumps(result))

# 3. Average likes per tweet
class AverageLikes(Resource):
    def get(self):
        term = request.args.get('term', '')
        collection = connect_to_mongodb()
        pipeline = [
            {"$match": {"text": {"$regex": term, "$options": "i"}}},
            {"$group": {
                "_id": None,
                "avg_likes": {"$avg": "$like_count"}
            }}
        ]
        result = list(collection.aggregate(pipeline))
        return json.loads(json_util.dumps(result))

# 4. Tweet locations by place ID
class TweetLocations(Resource):
    def get(self):
        term = request.args.get('term', '')
        collection = connect_to_mongodb()
        pipeline = [
            {"$match": {"text": {"$regex": term, "$options": "i"}}},
            {"$group": {
                "_id": "$place_id",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        result = list(collection.aggregate(pipeline))
        return json.loads(json_util.dumps(result))

# 5. Tweets by time of day (hour)
class TweetTimes(Resource):
    def get(self):
        term = request.args.get('term', '')
        collection = connect_to_mongodb()
        pipeline = [
            {"$match": {"text": {"$regex": term, "$options": "i"}}},
            {"$project": {
                "time_of_day": {"$dateToString": {"format": "%H:%M:%S", "date": "$created_at"}}
            }},
            {"$group": {
                "_id": "$time_of_day",
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        result = list(collection.aggregate(pipeline))
        return json.loads(json_util.dumps(result))

# 6. User with most tweets containing the search term
class TopUser(Resource):
    def get(self):
        term = request.args.get('term', '')
        collection = connect_to_mongodb()
        pipeline = [
            {"$match": {"text": {"$regex": term, "$options": "i"}}},
            {"$group": {
                "_id": "$author_handle",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        result = list(collection.aggregate(pipeline))
        return json.loads(json_util.dumps(result))

api.add_resource(TweetsPerDay, '/tweets_per_day')
api.add_resource(UniqueUsers, '/unique_users')
api.add_resource(AverageLikes, '/average_likes')
api.add_resource(TweetLocations, '/tweet_locations')
api.add_resource(TweetTimes, '/tweet_times')
api.add_resource(TopUser, '/top_user')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
