import os
import json
import psycopg2
from dotenv import load_dotenv
import logging
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(filename='logs/scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

PG_USER = os.getenv('POSTGRES_USER')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
PG_DB = os.getenv('POSTGRES_DB')
PG_HOST = os.getenv('POSTGRES_HOST')
PG_PORT = os.getenv('POSTGRES_PORT')
RAW_DATA_PATH = 'data/raw/telegram_messages'

def create_raw_table(cur):
    """Creates the raw_telegram_messages table if it doesn't exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_telegram_messages (
            id BIGINT PRIMARY KEY,
            sender_id BIGINT,
            date TIMESTAMP,
            message TEXT,
            views INTEGER,
            forwards INTEGER,
            replies_count INTEGER,
            has_media BOOLEAN,
            media_type VARCHAR(50),
            media_path TEXT,
            entities JSONB,
            channel_name TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logging.info("Ensured raw_telegram_messages table exists.")

def insert_raw_data(cur, data, channel_name):
    """Inserts a single message's data into the raw_telegram_messages table."""
    try:
        cur.execute("""
            INSERT INTO raw_telegram_messages (
                id, sender_id, date, message, views, forwards, replies_count,
                has_media, media_type, media_path, entities, channel_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                sender_id = EXCLUDED.sender_id,
                date = EXCLUDED.date,
                message = EXCLUDED.message,
                views = EXCLUDED.views,
                forwards = EXCLUDED.forwards,
                replies_count = EXCLUDED.replies_count,
                has_media = EXCLUDED.has_media,
                media_type = EXCLUDED.media_type,
                media_path = EXCLUDED.media_path,
                entities = EXCLUDED.entities,
                channel_name = EXCLUDED.channel_name,
                scraped_at = CURRENT_TIMESTAMP;
        """, (
            data['id'], data['sender_id'], data['date'], data['message'],
            data['views'], data['forwards'], data['replies_count'],
            data['has_media'], data['media_type'], data['media_path'],
            json.dumps(data['entities']), channel_name
        ))
        logging.debug(f"Inserted/Updated message ID {data['id']} for channel {channel_name}")
    except Exception as e:
        logging.error(f"Error inserting data for message ID {data['id']} from channel {channel_name}: {e}")

def load_data_to_postgres():
    """Loads raw JSON data from the data lake into PostgreSQL."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()
        create_raw_table(cur)

        for date_dir in os.listdir(RAW_DATA_PATH):
            date_path = os.path.join(RAW_DATA_PATH, date_dir)
            if os.path.isdir(date_path):
                for channel_dir in os.listdir(date_path):
                    channel_path = os.path.join(date_path, channel_dir)
                    if os.path.isdir(channel_path):
                        channel_name = channel_dir
                        for filename in os.listdir(channel_path):
                            if filename.endswith('.json') and filename.startswith('message_'):
                                filepath = os.path.join(channel_path, filename)
                                try:
                                    with open(filepath, 'r', encoding='utf-8') as f:
                                        message_data = json.load(f)
                                    insert_raw_data(cur, message_data, channel_name)
                                except json.JSONDecodeError as e:
                                    logging.error(f"Error decoding JSON from {filepath}: {e}")
                                except Exception as e:
                                    logging.error(f"Error processing file {filepath}: {e}")
        conn.commit()
        logging.info("Successfully loaded all raw data to PostgreSQL.")

    except psycopg2.Error as e:
        logging.critical(f"Database connection or operation error: {e}")
    except Exception as e:
        logging.critical(f"An unexpected error occurred during data loading: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_postgres()