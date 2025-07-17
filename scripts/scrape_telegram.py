import os
import sys
import logging
import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.tl.types import Channel, MessageMediaPhoto, MessageMediaDocument, DocumentAttributeFilename
from telethon.errors import SessionPasswordNeededError, FloodWaitError, PeerFloodError, ChannelPrivateError

# --- Configuration & Logging ---
load_dotenv()

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE_NUMBER")
SESSION_NAME = 'telegram_scraper_session' # Session file will be created in /app (project root)

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "/app/data/raw/telegram_messages")
RAW_IMAGES_DIR = os.getenv("RAW_IMAGES_DIR", "/app/data/raw/telegram_images")
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(RAW_IMAGES_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(os.path.join(LOG_DIR, 'scraper.log')),
                        logging.StreamHandler(sys.stdout)
                    ])
logger = logging.getLogger(__name__)

# List of Telegram channel identifiers (usernames or full links)
# IMPORTANT: For private channels, you must be a member first.
# For public channels, you can use their username (e.g., 'chemedtg') or full link.
TELEGRAM_CHANNELS = [
    'Chemedtg', # Example: Assuming 'Chemedtg' is the public username
    'https://t.me/lobelia4cosmetics',
    'https://t.me/tikvahpharma',
    # Add more channels from https://et.tgstat.com/medicine here
    # Example: 'https://t.me/AnotherMedicalChannel'
]

async def download_media(client, message, channel_name):
    """Downloads media (photos/documents) from a message."""
    if not message.media:
        return None, None # No media in this message

    media_path = None
    media_type = None

    try:
        # Create a channel-specific directory for images
        channel_image_dir = os.path.join(RAW_IMAGES_DIR, channel_name.replace(' ', '_'))
        os.makedirs(channel_image_dir, exist_ok=True)

        # Telethon's download_media saves the file to the specified path
        # It handles different media types (photo, video, document)
        download_file_path = await client.download_media(message, file=channel_image_dir)

        if download_file_path:
            media_path = os.path.basename(download_file_path) # Just the filename
            if isinstance(message.media, MessageMediaPhoto):
                media_type = 'photo'
            elif isinstance(message.media, MessageMediaDocument):
                # Check for document attributes to get original filename
                if message.media.document.attributes:
                    for attr in message.media.document.attributes:
                        if isinstance(attr, DocumentAttributeFilename):
                            media_path = attr.file_name # Use original filename
                            break
                media_type = 'document' # Could be video, audio, etc.
            else:
                media_type = 'unknown_media'
            logger.info(f"Downloaded media '{media_path}' from channel '{channel_name}'.")
            return media_path, media_type
    except Exception as e:
        logger.error(f"Error downloading media from message {message.id} in channel '{channel_name}': {e}", exc_info=False)
    return None, None

async def scrape_channel(client, channel_identifier):
    """Scrapes messages from a single Telegram channel."""
    try:
        entity = await client.get_entity(channel_identifier)
        if not isinstance(entity, Channel):
            logger.warning(f"'{channel_identifier}' is not a valid Telegram channel or group. Skipping.")
            return

        channel_name = entity.title
        channel_id = entity.id
        logger.info(f"Scraping channel: '{channel_name}' (ID: {channel_id})")

        today_str = datetime.now().strftime('%Y-%m-%d')
        output_dir = os.path.join(RAW_DATA_DIR, today_str)
        os.makedirs(output_dir, exist_ok=True)

        output_file_path = os.path.join(output_dir, f"{channel_name.replace(' ', '_').replace('/', '')}.json")
        scraped_messages = []
        message_count = 0

        # Check if the file already exists and load existing data to append (optional, for incremental)
        # For simplicity in this first version, we'll overwrite or assume daily fresh scrape.
        # If you want to append, you'd load existing JSON here and then skip messages already present.

        async for message in client.iter_messages(entity, limit=None): # Fetch all messages
            try:
                # Basic message parsing
                message_data = {
                    'message_id': message.id,
                    'date': message.date.isoformat(),
                    'text': message.text,
                    'sender_id': message.sender_id,
                    'views': message.views,
                    'media_path': None,
                    'media_type': None,
                    'is_sponsored': message.sponsored,
                    'replies_count': message.replies.replies if message.replies else 0,
                    'forwards': message.forwards,
                    'post_channel_id': entity.id,
                    'post_channel_name': entity.title,
                    'message_link': f"https://t.me/{entity.username}/{message.id}" if entity.username else None
                }

                # Image and other media scraping
                if message.media:
                    media_file_name, media_type = await download_media(client, message, channel_name)
                    message_data['media_path'] = media_file_name
                    message_data['media_type'] = media_type

                scraped_messages.append(message_data)
                message_count += 1

                if message_count % 100 == 0:
                    logger.info(f"Scraped {message_count} messages from '{channel_name}'...")

            except FloodWaitError as fwe:
                logger.warning(f"Flood wait error for '{channel_name}': {fwe}. Waiting {fwe.seconds} seconds...")
                await asyncio.sleep(fwe.seconds + 5) # Add a small buffer
            except PeerFloodError as pfe:
                logger.warning(f"Peer flood error for '{channel_name}': {pfe}. Waiting for an extended period...")
                await asyncio.sleep(60 * 5) # Wait 5 minutes
            except Exception as e:
                logger.error(f"Error processing message {message.id} from '{channel_name}': {e}", exc_info=True)

        # Save all scraped messages for this channel to JSON
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(scraped_messages, f, ensure_ascii=False, indent=4)
        logger.info(f"Finished scraping '{channel_name}'. Total messages: {message_count}. Data saved to {output_file_path}")

    except ChannelPrivateError:
        logger.error(f"Cannot access private channel '{channel_identifier}'. You must join it first via the Telegram app.")
    except FloodWaitError as fwe:
        logger.critical(f"Global Flood wait error: {fwe}. Script will wait and retry after {fwe.seconds} seconds.", exc_info=True)
        await asyncio.sleep(fwe.seconds + 10)
    except Exception as e:
        logger.critical(f"Failed to scrape channel '{channel_identifier}': {e}", exc_info=True)


async def main():
    if not API_ID or not API_HASH or not PHONE:
        logger.critical("TELEGRAM_API_ID, TELEGRAM_API_HASH, or TELEGRAM_PHONE_NUMBER not found in .env file. Please check your .env setup and replace placeholders.")
        sys.exit(1)

    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)

    try:
        logger.info("Connecting to Telegram...")
        await client.start(phone=PHONE)
        logger.info("Successfully connected to Telegram!")

        # Iterate through all defined channels and scrape
        for channel_identifier in TELEGRAM_CHANNELS:
            await scrape_channel(client, channel_identifier)
            await asyncio.sleep(5) # Small delay between channels to avoid rate limits

    except SessionPasswordNeededError:
        logger.critical("2FA is enabled. Please run interactively once to enter password or consider setting up a password in .env if running headless.")
        # To run interactively for password:
        # 1. Change command in docker-compose.yml to `command: bash`
        # 2. `docker-compose up` (without -d)
        # 3. Inside container: `python scripts/scrape_telegram.py`
        # 4. Enter password when prompted. The session file will be created.
    except Exception as e:
        logger.critical(f"Error during Telegram connection or authentication: {e}", exc_info=True)
    finally:
        if client.is_connected():
            logger.info("Disconnecting from Telegram...")
            await client.disconnect()
            logger.info("Disconnected.")

if __name__ == '__main__':
    asyncio.run(main())