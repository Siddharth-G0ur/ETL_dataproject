import os
import sys
import logging
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure, BulkWriteError
from bson import ObjectId
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_mongodb_client():
    mongodb_uri = os.environ.get('MONGODB_URI', 'mongodb://mongodb:27017/')
    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        return client
    except ConnectionFailure:
        logger.error(f"Failed to connect to MongoDB at {mongodb_uri}")
        sys.exit(1)

def convert_to_type(value, target_type, field_name):
    if value is None:
        return None
    try:
        if target_type == 'object':
            return value
        elif target_type == 'string':
            return str(value)
        elif target_type == 'number':
            return float(value)
        elif target_type == 'boolean':
            return bool(value)
        elif target_type == 'ObjectId':
            return ObjectId(value) if isinstance(value, str) else value
        elif target_type == 'Long':
            return int(value)
        elif target_type == 'ISODate':
            if isinstance(value, datetime):
                return value
            elif isinstance(value, str):
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            else:
                raise ValueError(f"Invalid date format for field {field_name}: {value}")
        else:
            return value
    except (ValueError, TypeError) as e:
        logger.warning(f"Conversion failed for field '{field_name}': {str(e)}. Original value: {value}")
        return value  # Return original value if conversion fails

def convert_data_types():
    client = get_mongodb_client()
    database_name = os.environ.get('DATABASE_NAME', 'potato_db')
    collection_name = os.environ.get('COLLECTION_NAME', 'tweets')

    db = client[database_name]
    collection = db[collection_name]

    field_types = {
        '_id': 'ObjectId',
        'id': 'Long',
        'event': 'string',
        'ts1': 'ISODate',
        'ts2': 'ISODate',
        'from_stream': 'boolean',
        'directly_from_stream': 'boolean',
        'from_search': 'boolean',
        'directly_from_search': 'boolean',
        'from_quote_search': 'boolean',
        'directly_from_quote_search': 'boolean',
        'from_convo_search': 'boolean',
        'directly_from_convo_search': 'boolean',
        'from_timeline_search': 'boolean',
        'directly_from_timeline_search': 'boolean',
        'text': 'string',
        'lang': 'string',
        'author_id': 'Long',
        'author_handle': 'string',
        'created_at': 'ISODate',
        'conversation_id': 'Long',
        'possibly_sensitive': 'boolean',
        'reply_settings': 'string',
        'source': 'string',
        'author_follower_count': 'number',
        'retweet_count': 'number',
        'reply_count': 'number',
        'like_count': 'number',
        'quote_count': 'number',
        'replied_to': 'string',
        'replied_to_author_id': 'string',
        'replied_to_handle': 'string',
        'replied_to_follower_count': 'string',
        'quoted': 'string',
        'quoted_author_id': 'string',
        'quoted_handle': 'string',
        'quoted_follower_count': 'string',
        'retweeted': 'string',
        'retweeted_author_id': 'string',
        'retweeted_handle': 'string',
        'retweeted_follower_count': 'string',
        'mentioned_author_ids': 'string',
        'mentioned_handles': 'string',
        'hashtags': 'string',
        'urls': 'string',
        'media_keys': 'string',
        'place_id': 'string'
    }

    try:
        cursor = collection.find({})
        bulk_operations = []
        batch_size = 1000
        total_processed = 0
        total_updated = 0
        conversion_stats = {field: {'attempts': 0, 'successes': 0} for field in field_types}

        for doc in cursor:
            updates = {}

            for field, target_type in field_types.items():
                if field in doc:
                    conversion_stats[field]['attempts'] += 1
                    converted_value = convert_to_type(doc[field], target_type, field)
                    if converted_value != doc[field]:
                        updates[field] = converted_value
                        conversion_stats[field]['successes'] += 1

            if updates:
                bulk_operations.append(UpdateOne({'_id': doc['_id']}, {'$set': updates}))

            total_processed += 1

            if len(bulk_operations) >= batch_size:
                result = collection.bulk_write(bulk_operations)
                total_updated += result.modified_count
                bulk_operations = []
                logger.info(f"Processed {total_processed} documents. Updated: {total_updated}")

        # Process any remaining operations
        if bulk_operations:
            result = collection.bulk_write(bulk_operations)
            total_updated += result.modified_count

        logger.info(f"Data type conversion completed successfully.")
        logger.info(f"Total processed: {total_processed}, Updated: {total_updated}")
        logger.info("Conversion statistics:")
        for field, stats in conversion_stats.items():
            if stats['attempts'] > 0:
                success_rate = (stats['successes'] / stats['attempts']) * 100
                logger.info(f"  {field}: {stats['successes']} successes out of {stats['attempts']} attempts ({success_rate:.2f}%)")

    except BulkWriteError as bwe:
        logger.error(f"Bulk write error: {bwe.details}")
    except OperationFailure as e:
        logger.error(f"An error occurred during data conversion: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    logger.info("Starting safe data type conversion process...")
    convert_data_types()
    logger.info("Safe data type conversion process finished.")