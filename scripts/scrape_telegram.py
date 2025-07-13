import os
import asyncio
import json
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')
session_name = 'telegram_scraper' # You can change this
raw_data_path = 'data/raw/telegram_messages'
logs_path = 'logs/scraper.log'

# Configure logging
logging.basicConfig(filename=logs_path, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# List of Telegram channels to scrape
# You'll need to join these channels first in your Telegram client
TELEGRAM_CHANNELS = [
    'Chemed_Telegram_Channel',  # Replace with actual username/ID
    'lobelia4cosmetics',        # Replace with actual username/ID
    'tikvahpharma'              # Replace with actual username/ID
    # Add more channels from https://et.tgstat.com/medicine
]

async def scrape_channel(client, channel_entity):
    """Scrapes messages and media from a single Telegram channel."""
    channel_name = channel_entity.title.replace(" ", "_")
    today = datetime.now().strftime('%Y-%m-%d')
    channel_dir = os.path.join(raw_data_path, today, channel_name)
    os.makedirs(channel_dir, exist_ok=True)
    logging.info(f"Starting scraping for channel: {channel_name}")

    try:
        # Get channel history
        async for message in client.iter_messages(channel_entity, limit=None): # Adjust limit as needed
            message_data = {
                'id': message.id,
                'sender_id': message.sender_id,
                'date': message.date.isoformat(),
                'message': message.message,
                'views': message.views,
                'forwards': message.forwards,
                'replies_count': message.replies.replies if message.replies else 0,
                'has_media': bool(message.media),
                'media_type': None,
                'media_path': None,
                'entities': [e.to_dict() for e in message.entities] if message.entities else []
            }

            if message.media:
                if isinstance(message.media, MessageMediaPhoto):
                    message_data['media_type'] = 'photo'
                elif isinstance(message.media, MessageMediaDocument):
                    message_data['media_type'] = 'document'
                # You can add more media types if needed

                # Download media
                try:
                    media_filename = f"{message.id}_{message_data['media_type']}.jpg" # Assuming most media for YOLO will be images
                    media_full_path = os.path.join(channel_dir, media_filename)
                    await client.download_media(message, file=media_full_path)
                    message_data['media_path'] = media_full_path
                    logging.info(f"Downloaded media for message {message.id} from {channel_name}")
                except Exception as e:
                    logging.warning(f"Could not download media for message {message.id} in channel {channel_name}: {e}")

            # Save message data to JSON
            message_filename = os.path.join(channel_dir, f"message_{message.id}.json")
            with open(message_filename, 'w', encoding='utf-8') as f:
                json.dump(message_data, f, ensure_ascii=False, indent=4)
            logging.debug(f"Saved message {message.id} from {channel_name}")

        logging.info(f"Finished scraping for channel: {channel_name}")

    except Exception as e:
        logging.error(f"Error scraping channel {channel_entity.title}: {e}")


async def main():
    """Main function to connect to Telegram and scrape all channels."""
    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()
    logging.info("Telegram client started.")

    for channel_input in TELEGRAM_CHANNELS:
        try:
            # Resolve channel entity (can be username or full link)
            if channel_input.startswith('https://t.me/'):
                channel_username = channel_input.split('/')[-1]
                channel_entity = await client.get_entity(channel_username)
            else:
                channel_entity = await client.get_entity(channel_input)

            await scrape_channel(client, channel_entity)
        except Exception as e:
            logging.error(f"Could not resolve or scrape channel {channel_input}: {e}")

    await client.run_until_disconnected()
    logging.info("Telegram client disconnected.")

if __name__ == '__main__':
    # Ensure raw data and logs directories exist
    os.makedirs(os.path.join(raw_data_path, datetime.now().strftime('%Y-%m-%d')), exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    asyncio.run(main())