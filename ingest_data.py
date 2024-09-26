import pandas as pd
from pymongo import MongoClient, ReplaceOne
import os
import sys
import gc
import logging
import psutil
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def print_versions():
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Pandas version: {pd.__version__}")

def connect_to_mongodb():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['potato_db']
    return db['tweets']

def validate_record(record):
    required_fields = ['id']
    return all(field in record and pd.notna(record[field]) for field in required_fields)

def log_memory_usage():
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    logger.info(f"Current memory usage: {memory_usage:.2f} MB")

def process_chunk(chunk, collection):
    records = chunk.to_dict('records')
    bulk_operations = [
        ReplaceOne({'id': record['id']}, record, upsert=True)
        for record in records
        if validate_record(record)
    ]
    return bulk_operations

def write_to_db(collection, bulk_operations):
    if bulk_operations:
        try:
            result = collection.bulk_write(bulk_operations, ordered=False)
            logger.info(f"Write result: {result.bulk_api_result}")  # Log the result of the write operation
            return result.bulk_api_result
        except Exception as e:
            logger.error(f"Error during bulk write: {str(e)}")
            return None


def count_lines(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

def ingest_data(file_path, collection):
    chunksize = 50000  # Adjust chunk size as needed
    total_processed, total_errors = 0, 0
    total_lines = count_lines(file_path)
    logger.info(f"Total lines in file: {total_lines}")

    try:
        with tqdm(total=total_lines, desc="Processing records") as pbar:
            for chunk in pd.read_csv(file_path, sep='\t', chunksize=chunksize, low_memory=False):
                bulk_operations = process_chunk(chunk, collection)
                processed = len(bulk_operations)
                errors = len(chunk) - processed
                
                if bulk_operations:
                    write_to_db(collection, bulk_operations)
                
                total_processed += processed
                total_errors += errors
                pbar.update(processed + errors)

                # Log progress
                remaining = total_lines - total_processed - total_errors
                logger.info(f"Processed: {total_processed}, Errors: {total_errors}, Remaining: {remaining}")
                log_memory_usage()

        logger.info(f"Total records processed: {total_processed}")
        logger.info(f"Total errors encountered: {total_errors}")
    except Exception as e:
        logger.error(f"Fatal error during data ingestion: {str(e)}")

def main():
    print_versions()
    logger.info("Starting data ingestion...")
    collection = connect_to_mongodb()

    # Create indexes for faster queries
    collection.create_index("id")
    collection.create_index("author_id")

    ingest_data('/app/data/correct_twitter_202102.tsv', collection)
    logger.info("Data ingestion completed.")
    logger.info(f"Total documents in collection: {collection.count_documents({})}")

if __name__ == "__main__":
    main()
