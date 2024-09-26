import pandas as pd
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_mongodb():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['potato_db']
    return db['tweets']

def query_tweets(collection, search_term):
    # Count tweets containing the search term by day
    daily_counts = collection.aggregate([
        {
            "$match": {
                "text": {"$regex": search_term, "$options": "i"},
                "created_at": {"$type": "date"}  # Ensure it's a date type
            }
        },
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ])

    # Count unique users who posted tweets containing the term
    unique_users = collection.distinct("author_id", {
        "text": {"$regex": search_term, "$options": "i"}
    })

    # Calculate average likes for tweets containing the term
    avg_likes = collection.aggregate([
        {
            "$match": {
                "text": {"$regex": search_term, "$options": "i"}
            }
        },
        {
            "$group": {
                "_id": None,
                "avgLikes": {"$avg": "$likes_count"}  # Assuming you have a 'likes_count' field
            }
        }
    ])

    # Get places of the tweets
    places = collection.distinct("place_id", {
        "text": {"$regex": search_term, "$options": "i"}
    })

    # Get times of day tweets were posted
    times_of_day = collection.aggregate([
        {
            "$match": {
                "text": {"$regex": search_term, "$options": "i"}
            }
        },
        {
            "$group": {
                "_id": {"$hour": "$created_at"},
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ])

    # Find user who posted the most tweets containing the term
    most_active_user = collection.aggregate([
        {
            "$match": {
                "text": {"$regex": search_term, "$options": "i"}
            }
        },
        {
            "$group": {
                "_id": "$author_id",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        },
        {
            "$limit": 1
        }
    ])

    # Output results
    logger.info(f"Daily counts for '{search_term}': {list(daily_counts)}")
    logger.info(f"Unique users for '{search_term}': {len(unique_users)}")
    logger.info(f"Average likes for '{search_term}': {list(avg_likes)}")
    logger.info(f"Places for '{search_term}': {places}")
    logger.info(f"Times of day for '{search_term}': {list(times_of_day)}")
    logger.info(f"Most active user for '{search_term}': {list(most_active_user)}")

def main():
    collection = connect_to_mongodb()
    search_term = input("Enter a search term (e.g., 'music'): ")
    query_tweets(collection, "music")

if __name__ == "__main__":
    main()
