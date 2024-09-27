import pandas as pd
from pymongo import MongoClient
import logging
import psutil
import os
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_mongodb():
    """
    Establishes a connection to MongoDB and returns the tweets collection.
    """
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['potato_db']
    return db['tweets']

def log_memory_usage():
    """
    Logs the current memory usage of the process.
    """
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    logger.info(f"Current memory usage: {memory_usage:.2f} MB")

def load_and_clean_data(file_path):
    """
    Loads data from a TSV file and cleans it.
    
    Args:
    file_path (str): Path to the TSV file.
    
    Returns:
    pd.DataFrame: Cleaned DataFrame.
    """
    # Load the TSV file
    df = pd.read_csv(file_path, sep='\t')
    
    # Clean 'created_at' column
    df['created_at'] = df['created_at'].astype(str).str.strip()
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)

    # Strip leading/trailing whitespace for all string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Define expected data types for each column
    expected_dtypes = {
        'id': 'int64',
        'event': 'object',
        'ts1': 'datetime64[ns]',
        ' ts2': 'datetime64[ns]', 
        'from_stream': 'bool',
        'directly_from_stream': 'bool',
        'from_search': 'bool',
        'directly_from_search': 'bool',
        'from_quote_search': 'bool',
        'directly_from_quote_search': 'bool',
        'from_convo_search': 'bool',
        'directly_from_convo_search': 'bool',
        'from_timeline_search': 'bool',
        'directly_from_timeline_search': 'bool',
        'text': 'object',
        'lang': 'object',
        'author_id': 'int64',
        'author_handle': 'object',
        'created_at': 'datetime64[ns]',
        'conversation_id': 'int64',
        'possibly_sensitive': 'bool',
        'reply_settings': 'object',
        'source': 'object',
        'author_follower_count': 'int64',
        'retweet_count': 'int64',
        'reply_count': 'int64',
        'like_count': 'int64',
        'quote_count': 'int64',
        'replied_to': 'float64',
        'replied_to_author_id': 'float64',
        'replied_to_handle': 'object',
        'replied_to_follower_count': 'float64',
        'quoted': 'float64',
        'quoted_author_id': 'float64',
        'quoted_handle': 'object',
        'quoted_follower_count': 'float64',
        'retweeted': 'float64',
        'retweeted_author_id': 'float64',
        'retweeted_handle': 'object',
        'retweeted_follower_count': 'float64',
        'mentioned_author_ids': 'object',
        'mentioned_handles': 'object',
        'hashtags': 'object',
        'urls': 'object',
        'media_keys': 'object',
        'place_id': 'object',
    }

    # Clean and convert each column according to expected data types
    for column, dtype in expected_dtypes.items():
        if column in df.columns:
            if dtype == 'int64':
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype('int64')
            elif dtype == 'datetime64[ns]':
                df[column] = pd.to_datetime(df[column], errors='coerce', utc=True)

    # Remove rows with NaT in 'created_at'
    df = df.dropna(subset=['created_at'])

    return df

def filter_tweets(df, search_term):
    """
    Filters tweets containing a specific search term.
    
    Args:
    df (pd.DataFrame): DataFrame containing tweets.
    search_term (str): Term to search for in tweets.
    
    Returns:
    pd.DataFrame: Filtered DataFrame.
    """
    filtered_tweets = df[df['text'].str.contains(search_term, case=False, na=False)]
    return filtered_tweets

def ingest_data_in_chunks(df, collection, chunk_size=5000):
    """
    Ingests data into MongoDB in chunks to manage memory usage.
    
    Args:
    df (pd.DataFrame): DataFrame to ingest.
    collection (pymongo.collection.Collection): MongoDB collection to insert into.
    chunk_size (int): Number of records to insert in each chunk.
    """
    total_rows = len(df)
    logger.info(f"Total rows to insert: {total_rows}")
    
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]
        
        records = chunk.to_dict(orient='records')
        
        if records:
            try:
                collection.insert_many(records, ordered=False)
                logger.info(f"Inserted rows {start} to {end}")
            except Exception as e:
                logger.error(f"Error during insert: {str(e)}")
        
        log_memory_usage()  # Log memory usage after each chunk

def main():
    """
    Main function to orchestrate the data ingestion process.
    """
    # Load and clean the data
    file_path = 'data/correct_twitter_202102.tsv' 
    df = load_and_clean_data(file_path)

    logger.info("Starting data ingestion...")
    collection = connect_to_mongodb()
    collection.create_index("id")
    collection.create_index("author_id")
    
    # Ingest the cleaned DataFrame into MongoDB
    ingest_data_in_chunks(df, collection)

    logger.info("Data ingestion completed.")
    logger.info(f"Total documents in collection: {collection.count_documents({})}")

if __name__ == "__main__":
    main()
