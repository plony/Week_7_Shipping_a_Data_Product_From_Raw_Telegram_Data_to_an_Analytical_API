import os
import json
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration from .env ---
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
RAW_DATA_DIR = os.getenv('RAW_DATA_DIR', 'data/raw/telegram_messages') # Default if not set

# Ensure the raw data directory exists
if not os.path.exists(RAW_DATA_DIR):
    print(f"Error: Raw data directory '{RAW_DATA_DIR}' not found. Please ensure your Telegram JSON files are here.")
    exit(1)

def create_raw_table(cursor):
    """Creates the raw schema and telegram_messages table if they don't exist."""
    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            id SERIAL PRIMARY KEY,
            message_json JSONB NOT NULL,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)
    print("Ensured raw.telegram_messages table exists.")

def load_json_to_db(file_path, cursor):
    """Loads a single JSON file into the raw.telegram_messages table."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, list):
            data = [data] # Ensure data is a list of messages

        for message in data:
            # Check if message with this 'id' already exists to prevent duplicates
            message_id = message.get('id')
            if message_id is None:
                print(f"Warning: Skipping message in {file_path} due to missing 'id'.")
                continue

            # Check for existing message by its original Telegram ID (if available and reliable)
            # For simplicity, if we are loading entire files, we might just insert,
            # but for de-duplication, checking the original message ID is better.
            # Assuming 'id' in JSON is a unique identifier from Telegram.
            cursor.execute(
                "SELECT id FROM raw.telegram_messages WHERE (message_json->>'id')::BIGINT = %s;",
                (message_id,)
            )
            if cursor.fetchone():
                print(f"Message with original ID {message_id} from {file_path} already exists. Skipping.")
                continue

            cursor.execute(
                "INSERT INTO raw.telegram_messages (message_json) VALUES (%s);",
                (json.dumps(message),) # psycogp2 expects string for JSONB
            )
            print(f"Loaded message {message_id} from {file_path}")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {file_path}: {e}")
    except Exception as e:
        print(f"Error loading {file_path}: {e}")

def main():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        conn.autocommit = True # Auto-commit each transaction for simplicity in loading

        with conn.cursor() as cursor:
            create_raw_table(cursor)

            for root, _, files in os.walk(RAW_DATA_DIR):
                for file_name in files:
                    if file_name.endswith('.json'):
                        file_path = os.path.join(root, file_name)
                        print(f"Processing file: {file_path}")
                        load_json_to_db(file_path, cursor)
        print("Finished loading raw JSON data.")

    except Exception as e:
        print(f"Database connection or operation error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()