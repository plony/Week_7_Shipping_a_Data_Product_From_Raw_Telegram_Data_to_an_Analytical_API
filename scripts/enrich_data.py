import os
import psycopg2
import json
from ultralytics import YOLO
from dotenv import load_dotenv
import logging
from PIL import Image

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

# Load a pre-trained YOLO model (e.g., YOLOv8n for 'nano' version)
# You might need to download this model once, or it will download automatically.
# Consider using a model specifically fine-tuned for medical products if available.
model = YOLO('yolov8n.pt')

def get_messages_with_media_paths():
    """Fetches messages with media paths from the raw_telegram_messages table."""
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
        cur.execute("""
            SELECT id, media_path FROM raw_telegram_messages
            WHERE has_media = TRUE AND media_path IS NOT NULL AND media_path != '';
        """)
        return cur.fetchall()
    except psycopg2.Error as e:
        logging.critical(f"Database connection or operation error: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_detected_objects(message_id, detected_objects_json):
    """Updates the detected_objects column in fct_messages."""
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
        cur.execute("""
            UPDATE fct_messages
            SET detected_objects = %s
            WHERE message_id = %s;
        """, (detected_objects_json, message_id))
        conn.commit()
        logging.info(f"Updated detected objects for message_id: {message_id}")
    except psycopg2.Error as e:
        logging.error(f"Error updating detected objects for message_id {message_id}: {e}")
    finally:
        if conn:
            conn.close()

def run_object_detection():
    """Runs YOLO object detection on downloaded images and updates the database."""
    messages_to_process = get_messages_with_media_paths()
    if not messages_to_process:
        logging.info("No messages with media paths found for object detection.")
        return

    logging.info(f"Found {len(messages_to_process)} messages with media to process for object detection.")

    for message_id, media_path in messages_to_process:
        if not media_path or not os.path.exists(media_path):
            logging.warning(f"Media path {media_path} for message {message_id} does not exist. Skipping.")
            continue

        try:
            # Check if the file is a valid image before processing
            with Image.open(media_path) as img:
                img.verify() # Verify that it is an image
            
            # Perform inference
            results = model(media_path)

            detected_objects = []
            for r in results:
                for box in r.boxes:
                    class_id = int(box.cls[0])
                    class_name = model.names[class_id]
                    confidence = float(box.conf[0])
                    # You might want to save bounding box coordinates as well
                    detected_objects.append({
                        'class_name': class_name,
                        'confidence': confidence
                    })

            if detected_objects:
                update_detected_objects(message_id, json.dumps(detected_objects))
            else:
                logging.info(f"No objects detected in media for message_id: {message_id}")

        except (IOError, SyntaxError) as e:
            logging.error(f"File at {media_path} for message {message_id} is not a valid image: {e}")
        except Exception as e:
            logging.error(f"Error processing media for message_id {message_id} at {media_path}: {e}")

if __name__ == "__main__":
    run_object_detection()