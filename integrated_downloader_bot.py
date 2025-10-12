#!/usr/bin/env python3
"""
Integrated Downloader Bot
Downloads videos from video_data table, uploads to DiskWala, and sends to Telegram group
"""

import os
import asyncio
import re
import time
import sys
import requests
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from database import DatabaseManager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------- CONFIG --------------------
# Telegram Bot Configuration
API_ID = 27037965
API_HASH = "1d00df45695b0bf82f46328573fbfd22"
PHONE = "+917573911205"
SESSION_NAME = "session_diskwala"

BOT_USERNAME = "DiskWalaFileUploaderBot"
API_CMD = "/api 68e7e8def42a4241739ba1c7"

# Telegram group to send messages to
TELEGRAM_GROUP_ID = -1003172525478  # Replace with your group ID

# DiskWala pattern
DISKWALA_PATTERN = re.compile(
    r'(https?://(?:www\.)?diskwala\.com/app/[A-Za-z0-9_\-]+)', re.IGNORECASE
)

# Download settings
DOWNLOAD_DIR = "downloads"
DELAY_BETWEEN_UPLOADS = 2.0
MAX_FILE_SIZE = None  # None for no limit

# -------------------- CLASSES --------------------

class IntegratedDownloaderBot:
    def __init__(self):
        self.db = DatabaseManager()
        self.download_dir = DOWNLOAD_DIR
        self.urls_found = []
        
        # Create download directory
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
        
        # Setup requests session
        self.session = requests.Session()
        headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://www.eporner.com/",
        }
        self.session.headers.update(headers)
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def download_file(self, url, filename):
        """Download a file from URL"""
        try:
            filepath = os.path.join(self.download_dir, filename)
            
            # Check if file already exists
            if os.path.exists(filepath):
                logger.info(f"File already exists: {filename}")
                return filepath
            
            logger.info(f"Downloading: {url}")
            
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Get file size for progress tracking
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Show progress
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            print(f"\rProgress: {progress:.1f}% ({downloaded}/{total_size} bytes)", end="", flush=True)
            
            print(f"\n✓ Downloaded: {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None
    
    def human_size(self, n):
        """Convert bytes to human readable format"""
        for unit in ['B','KB','MB','GB','TB']:
            if n < 1024.0:
                return f"{n:3.1f}{unit}"
            n /= 1024.0
        return f"{n:.1f}PB"
    
    def cleanup_files(self, mp4_path, jpg_path):
        """Delete MP4 and JPG files after successful upload and posting"""
        try:
            deleted_files = []
            
            # Delete MP4 file
            if mp4_path and os.path.exists(mp4_path):
                os.remove(mp4_path)
                deleted_files.append(os.path.basename(mp4_path))
                logger.info(f"🗑️  Deleted MP4 file: {os.path.basename(mp4_path)}")
            
            # Delete JPG file
            if jpg_path and os.path.exists(jpg_path):
                os.remove(jpg_path)
                deleted_files.append(os.path.basename(jpg_path))
                logger.info(f"🗑️  Deleted JPG file: {os.path.basename(jpg_path)}")
            
            if deleted_files:
                logger.info(f"✅ Cleanup completed: {', '.join(deleted_files)}")
            else:
                logger.info("ℹ️  No files to cleanup")
                
            return True
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return False
    
    async def setup_telegram_client(self):
        """Setup Telegram client"""
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await client.start(phone=PHONE)
        
        try:
            # Ensure bot is reachable
            logger.info(f"Resolving bot @{BOT_USERNAME} ...")
            bot_entity = await client.get_entity(BOT_USERNAME)
            
            # Send the /api command once
            logger.info(f"Sending API command to @{BOT_USERNAME}: {API_CMD}")
            await client.send_message(bot_entity, API_CMD)
            logger.info("Command sent. Waiting for bot reply...")
            await asyncio.sleep(3)
            
            return client, bot_entity
            
        except Exception as e:
            logger.error(f"Failed to setup Telegram client: {e}")
            await client.disconnect()
            return None, None
    
    async def upload_to_diskwala(self, client, bot_entity, filepath):
        """Upload file to DiskWala and get URL"""
        try:
            filename = os.path.basename(filepath)
            fsize = os.path.getsize(filepath)
            
            logger.info(f"Uploading: {filename} ({self.human_size(fsize)})")
            
            # Progress callback
            last_print = 0
            def progress(current, total):
                nonlocal last_print
                if total == 0:
                    return
                pct = int(current * 100 / total)
                now = time.time()
                if pct != last_print or now - last_print_time.get('t', 0) > 0.5:
                    last_print = pct
                    last_print_time['t'] = now
                    sys.stdout.write(f"\r  Progress: {pct}% ({self.human_size(current)}/{self.human_size(total)})")
                    sys.stdout.flush()
            
            last_print_time = {'t': 0}
            
            # Upload file
            await client.send_file(bot_entity, filepath, caption=filename, progress_callback=progress)
            print("\n  ✅ Upload completed. Waiting for DiskWala URL...")
            
            # Wait for DiskWala URL
            await asyncio.sleep(5)
            
            return True
            
        except FloodWaitError as e:
            logger.warning(f"Flood wait: must wait {e.seconds} seconds")
            await asyncio.sleep(e.seconds + 1)
            return await self.upload_to_diskwala(client, bot_entity, filepath)
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False
    
    async def send_to_telegram_group(self, client, group_id, jpg_path, diskwala_url, video_info):
        """Send JPG image and DiskWala URL to Telegram group"""
        try:
            # Get group entity
            group_entity = await client.get_entity(group_id)
            
            # Create message text
            message_text = f"""🎬 **New Video Uploaded!**

📥 𝐃𝐨𝐰𝐧𝐥𝐨𝐚𝐝 𝐋𝐢𝐧𝐤𝐬/👀𝐖𝐚𝐭𝐜𝐡 𝐎𝐧𝐥𝐢𝐧𝐞 🍑

👇

🔗 **Download Link:** {diskwala_url}

.

𝗘𝗻𝗷𝗼𝘆 ♥️🍑✌️

"""
            
            # Send image with caption
            if os.path.exists(jpg_path):
                await client.send_file(group_entity, jpg_path, caption=message_text, parse_mode='md')
                logger.info(f"Sent to Telegram group: {diskwala_url}")
            else:
                # Send text message if image not available
                await client.send_message(group_entity, message_text, parse_mode='md')
                logger.info(f"Sent text message to Telegram group: {diskwala_url}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending to Telegram group: {e}")
            return False
    
    async def process_videos(self):
        """Main process to download, upload, and send videos"""
        try:
            # Get video data from database
            video_data_list = self.db.get_video_data_for_download(limit=5)
            
            if not video_data_list:
                logger.info("No videos to process")
                return
            
            logger.info(f"Found {len(video_data_list)} videos to process")
            
            # Setup Telegram client
            client, bot_entity = await self.setup_telegram_client()
            if not client or not bot_entity:
                logger.error("Failed to setup Telegram client")
                return
            
            # Set up URL monitoring
            @client.on(events.NewMessage(chats=bot_entity))
            async def url_handler(event):
                if event.message and event.message.message:
                    text = event.message.message
                    for url in DISKWALA_PATTERN.findall(text):
                        self.urls_found.append(url)
                        logger.info(f"🎉 DiskWala URL Found: {url}")
            
            logger.info("📡 Monitoring chat for DiskWala URLs...")
            
            # Process each video
            for idx, video_data in enumerate(video_data_list, start=1):
                try:
                    logger.info(f"\n[{idx}/{len(video_data_list)}] Processing video: {video_data['video_url']}")
                    
                    # Check if video is already uploaded to DiskWala
                    if self.db.is_video_already_uploaded(video_data['video_url']):
                        logger.info(f"⏭️  Video already uploaded to DiskWala, skipping: {video_data['video_url']}")
                        continue
                    
                    # Get MP4 and JPG links
                    mp4_links = video_data['mp4_links']
                    jpg_links = video_data['jpg_links']
                    
                    if not mp4_links or not jpg_links:
                        logger.warning(f"No MP4 or JPG links found for: {video_data['video_url']}")
                        continue
                    
                    # Download MP4 file
                    mp4_url = mp4_links[0]  # Get first MP4 link
                    mp4_filename = f"video_{video_data['id']}.mp4"
                    mp4_path = self.download_file(mp4_url, mp4_filename)
                    
                    if not mp4_path:
                        logger.error(f"Failed to download MP4: {mp4_url}")
                        continue
                    
                    # Download JPG file
                    jpg_url = jpg_links[0]  # Get first JPG link
                    jpg_filename = f"image_{video_data['id']}.jpg"
                    jpg_path = self.download_file(jpg_url, jpg_filename)
                    
                    if not jpg_path:
                        logger.error(f"Failed to download JPG: {jpg_url}")
                        # Clean up MP4 file if JPG download failed
                        self.cleanup_files(mp4_path, None)
                        continue
                    
                    # Upload to DiskWala
                    upload_success = await self.upload_to_diskwala(client, bot_entity, mp4_path)
                    
                    if not upload_success:
                        logger.error(f"Failed to upload to DiskWala: {mp4_path}")
                        # Clean up files if upload failed
                        self.cleanup_files(mp4_path, jpg_path)
                        continue
                    
                    # Wait for DiskWala URL
                    await asyncio.sleep(10)
                    
                    # Check if we got a DiskWala URL
                    if self.urls_found:
                        diskwala_url = self.urls_found[-1]  # Get latest URL
                        
                        # Save to database
                        self.db.save_diskwala_data(
                            diskwala_url=diskwala_url,
                            jpg_image_link=jpg_url,
                            mp4_link=mp4_url,
                            video_url=video_data['video_url']
                        )
                        
                        # Send to Telegram group
                        await self.send_to_telegram_group(
                            client, TELEGRAM_GROUP_ID, jpg_path, diskwala_url, video_data
                        )
                        
                        logger.info(f"✅ Successfully processed: {video_data['video_url']}")
                        
                        # Clean up downloaded files after successful upload and posting
                        self.cleanup_files(mp4_path, jpg_path)
                    else:
                        logger.warning(f"No DiskWala URL received for: {video_data['video_url']}")
                        # Clean up files even if DiskWala URL not received
                        self.cleanup_files(mp4_path, jpg_path)
                    
                    # Delay between uploads
                    if idx < len(video_data_list):
                        await asyncio.sleep(DELAY_BETWEEN_UPLOADS)
                
                except Exception as e:
                    logger.error(f"Error processing video {video_data['video_url']}: {e}")
                    continue
            
            # Summary
            logger.info(f"\n📊 Summary:")
            logger.info(f"   Videos processed: {len(video_data_list)}")
            logger.info(f"   DiskWala URLs found: {len(self.urls_found)}")
            
            if self.urls_found:
                logger.info(f"\n🔗 All DiskWala URLs:")
                for i, url in enumerate(self.urls_found, 1):
                    logger.info(f"   {i}. {url}")
            
            logger.info("\n✅ All done. Disconnecting.")
            await client.disconnect()
            
        except Exception as e:
            logger.error(f"Error in process_videos: {e}")

async def main():
    """Main function"""
    bot = IntegratedDownloaderBot()
    await bot.process_videos()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user. Exiting.")
    except Exception as e:
        logger.error(f"Error in main: {e}")
