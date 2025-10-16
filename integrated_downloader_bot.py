#!/usr/bin/env python3
"""
Minimal Downloader Bot - Downloads videos and uploads to DiskWala
"""

import os
import asyncio
import re
import time
import sys
import requests
import subprocess
import shutil
import random
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from database import DatabaseManager
import logging
import urllib3

# Disable SSL warnings for server compatibility
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

# Proxy Configuration (from optimized_downloader.py)
PROXY_IP = "192.241.125.223"
PROXY_PORT = "8267"
PROXY_USERNAME = "tambmpew"
PROXY_PASSWORD = "sshljzu7jder"
USE_PROXY = True  # Set to False to disable proxy

# DiskWala pattern
DISKWALA_PATTERN = re.compile(
    r'(https?://(?:www\.)?diskwala\.com/app/[A-Za-z0-9_\-]+)', re.IGNORECASE
)

# Download settings
DOWNLOAD_DIR = "downloads"
DELAY_BETWEEN_UPLOADS = 2.0
MAX_FILE_SIZE_MB = 100  # Maximum file size in MB (set to None for no limit)
MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024 if MAX_FILE_SIZE_MB else None  # Convert to bytes
DOWNLOAD_TIMEOUT = 600  # 10 minutes timeout for large files
MAX_DOWNLOAD_ATTEMPTS = 5  # More attempts for large files

# Quality modification settings
MODIFY_QUALITY = True  # Set to False to disable quality modification
QUALITY_FROM = "480"   # Quality to replace
QUALITY_TO = "240"     # Quality to replace with

# Proxy rotation settings
PROXY_ROTATION_MIN = 10  # Minimum videos before proxy change
PROXY_ROTATION_MAX = 20  # Maximum videos before proxy change

# -------------------- CLASSES --------------------

class IntegratedDownloaderBot:
    def __init__(self):
        self.db = DatabaseManager()
        self.download_dir = DOWNLOAD_DIR
        self.urls_found = []
        self.current_proxy = None
        self.videos_processed_with_current_proxy = 0
        self.next_proxy_change_at = random.randint(PROXY_ROTATION_MIN, PROXY_ROTATION_MAX)
        
        # Create download directory
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
        
        # Setup requests session with server-optimized settings
        self.session = requests.Session()
        
        # Configure proxy if enabled
        if USE_PROXY:
            self.set_random_proxy()
        else:
            self.proxies = None
            logger.info("🚫 Proxy disabled")
        
        # Log file size limit
        if MAX_FILE_SIZE:
            logger.info(f"📏 File size limit: {self.human_size(MAX_FILE_SIZE)}")
        else:
            logger.info("📏 File size limit: None (no limit)")
        
        # Log quality modification settings
        if MODIFY_QUALITY:
            logger.info(f"🔄 Quality modification: {QUALITY_FROM}p → {QUALITY_TO}p")
        else:
            logger.info("🔄 Quality modification: Disabled")
        
        # Rotate User-Agents to avoid detection
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
        ]
        
        headers = {
            "User-Agent": user_agents[0],  # Will rotate in download_file
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
            "Referer": "https://www.eporner.com/",
        }
        self.session.headers.update(headers)
        
        # Enhanced retry strategy for server environments with large file support
        retry_strategy = Retry(
            total=3,  # Reduced retries to avoid long waits
            backoff_factor=2,  # Shorter backoff
            status_forcelist=[429, 500, 502, 503, 504, 403, 408],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            raise_on_status=False,
            read=0,  # Don't retry on read errors (let our custom logic handle it)
            connect=3,  # Retry connection errors
            redirect=3  # Retry redirects
        )
        
        # Configure adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
            pool_block=False
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Store user agents for rotation
        self.user_agents = user_agents
        self.current_ua_index = 0
    
    def set_random_proxy(self):
        """Set a random proxy from database"""
        try:
            proxy = self.db.get_random_proxy()
            if proxy:
                self.current_proxy = proxy
                self.proxies = {
                    'http': f"http://{proxy['username']}:{proxy['password']}@{proxy['ip']}:{proxy['port']}",
                    'https': f"http://{proxy['username']}:{proxy['password']}@{proxy['ip']}:{proxy['port']}"
                }
                self.session.proxies.update(self.proxies)
                logger.info(f"🔗 Using proxy: {proxy['ip']}:{proxy['port']} ({proxy['country']})")
                return True
            else:
                logger.warning("⚠️  No proxies available in database")
                return False
        except Exception as e:
            logger.error(f"❌ Error setting random proxy: {e}")
            return False
    
    def should_rotate_proxy(self):
        """Check if proxy should be rotated based on video count"""
        return self.videos_processed_with_current_proxy >= self.next_proxy_change_at
    
    def rotate_proxy_if_needed(self):
        """Rotate proxy if needed and reset counters"""
        if self.should_rotate_proxy():
            logger.info(f"🔄 Rotating proxy after {self.videos_processed_with_current_proxy} videos")
            if self.set_random_proxy():
                self.videos_processed_with_current_proxy = 0
                self.next_proxy_change_at = random.randint(PROXY_ROTATION_MIN, PROXY_ROTATION_MAX)
                logger.info(f"🎯 Next proxy change in {self.next_proxy_change_at} videos")
                return True
            else:
                logger.warning("⚠️  Failed to rotate proxy, continuing with current proxy")
                return False
        return True
    
    def test_proxy_with_eporner(self):
        """Test current proxy with eporner.com"""
        if not self.current_proxy:
            return False
            
        try:
            test_url = "https://www.eporner.com/"
            response = self.session.get(test_url, timeout=15)
            
            if response.status_code == 200:
                logger.info(f"✅ Proxy test successful with eporner.com")
                self.db.mark_proxy_success(self.current_proxy['proxy_id'])
                return True
            else:
                logger.warning(f"⚠️  Proxy test failed with eporner.com (status: {response.status_code})")
                self.db.mark_proxy_failure(self.current_proxy['proxy_id'])
                return False
                
        except Exception as e:
            logger.warning(f"⚠️  Proxy test failed with eporner.com: {e}")
            self.db.mark_proxy_failure(self.current_proxy['proxy_id'])
            return False
    
    def download_file(self, url, filename, max_attempts=None):
        """Download a file from URL with server-optimized retry logic"""
        if max_attempts is None:
            max_attempts = MAX_DOWNLOAD_ATTEMPTS
            
        filepath = os.path.join(self.download_dir, filename)
        
        # Check if file already exists
        if os.path.exists(filepath):
            logger.info(f"File already exists: {filename}")
            return filepath
        
        for attempt in range(max_attempts):
            try:
                # Rotate User-Agent for each attempt
                self.current_ua_index = (self.current_ua_index + 1) % len(self.user_agents)
                current_headers = self.session.headers.copy()
                current_headers['User-Agent'] = self.user_agents[self.current_ua_index]
                
                logger.info(f"Downloading: {url} (attempt {attempt + 1}/{max_attempts})")
                logger.info(f"Using User-Agent: {current_headers['User-Agent'][:50]}...")
                
                # Add random delay to avoid rate limiting
                if attempt > 0:
                    delay = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
                    logger.info(f"Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                
                # Make request with rotated headers and optimized timeouts for large files
                response = self.session.get(
                    url, 
                    stream=True, 
                    timeout=(30, DOWNLOAD_TIMEOUT),  # Use configured timeout for large files
                    headers=current_headers,
                    allow_redirects=True,
                    verify=False  # Disable SSL verification for server compatibility
                )
                
                # Check response status
                if response.status_code == 403:
                    logger.warning(f"Access forbidden (403) - server might be blocking requests")
                    continue
                elif response.status_code == 429:
                    logger.warning(f"Rate limited (429) - waiting longer...")
                    time.sleep(10)
                    continue
                elif response.status_code == 503:
                    logger.warning(f"Server overloaded (503) - retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                
                response.raise_for_status()
                
                # Get file size for progress tracking
                total_size = int(response.headers.get('content-length', 0))
                logger.info(f"File size: {self.human_size(total_size) if total_size > 0 else 'Unknown'}")
                
                # Check file size limit after getting actual response
                if MAX_FILE_SIZE and total_size > MAX_FILE_SIZE:
                    file_size_mb = total_size / (1024 * 1024)
                    logger.warning(f"⚠️  File too large ({self.human_size(total_size)} > {self.human_size(MAX_FILE_SIZE)}) - stopping download")
                    response.close()  # Close the connection to stop download
                    # Note: The calling code will handle marking as processed
                    return None
                
                # Validate content type for MP4 files (from optimized_downloader.py)
                if filename.endswith('.mp4'):
                    content_type = response.headers.get('content-type', '').lower()
                    if 'video' not in content_type and 'mp4' not in content_type:
                        logger.warning(f"Unexpected content type: {content_type}")
                        logger.warning(f"Server did not return a video file. Content-Type is '{content_type}'. Skipping.")
                        if attempt < max_attempts - 1:
                            continue
                        else:
                            logger.error(f"Failed: Server did not return a video file after {max_attempts} attempts")
                            return None
                
                # Check if file already exists and resume download
                downloaded = 0
                chunk_size = 8192  # Smaller chunk size for better stability
                
                if os.path.exists(filepath):
                    downloaded = os.path.getsize(filepath)
                    logger.info(f"Resuming download from {self.human_size(downloaded)}")
                    
                    # Check if partial file is already too large
                    if MAX_FILE_SIZE and downloaded > MAX_FILE_SIZE:
                        logger.warning(f"⚠️  Partial file already too large ({self.human_size(downloaded)} > {self.human_size(MAX_FILE_SIZE)}) - removing")
                        os.remove(filepath)
                        downloaded = 0
                    else:
                        # Add Range header for resume
                        current_headers['Range'] = f'bytes={downloaded}-'
                        response = self.session.get(url, headers=current_headers, stream=True, timeout=(30, DOWNLOAD_TIMEOUT))
                        
                        # Re-check file size for resumed download
                        resumed_total_size = int(response.headers.get('content-length', 0))
                        if resumed_total_size > 0:
                            actual_total_size = downloaded + resumed_total_size
                            logger.info(f"Resumed file total size will be: {self.human_size(actual_total_size)}")
                            
                            if MAX_FILE_SIZE and actual_total_size > MAX_FILE_SIZE:
                                logger.warning(f"⚠️  Resumed file would be too large ({self.human_size(actual_total_size)} > {self.human_size(MAX_FILE_SIZE)}) - stopping")
                                response.close()
                                os.remove(filepath)  # Remove partial file
                                return None
                
                with open(filepath, 'ab' if downloaded > 0 else 'wb') as f:
                    try:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                # Show progress
                                if total_size > 0:
                                    progress = (downloaded / total_size) * 100
                                    print(f"\rProgress: {progress:.1f}% ({self.human_size(downloaded)}/{self.human_size(total_size)})", end="", flush=True)
                                
                                # Flush periodically to ensure data is written
                                if downloaded % (chunk_size * 100) == 0:  # Every 100 chunks
                                    f.flush()
                                    
                    except Exception as download_error:
                        logger.warning(f"Download interrupted: {download_error}")
                        # If download was interrupted, try to resume
                        if downloaded > 0 and downloaded < total_size:
                            logger.info(f"Attempting to resume download from {self.human_size(downloaded)}")
                            raise download_error  # Let the retry logic handle it
                        else:
                            raise download_error
                
                print(f"\n✓ Downloaded: {filename} ({self.human_size(downloaded)})")
                
                # Validate downloaded file
                if filename.endswith('.mp4') and downloaded < 1024 * 1024:  # Less than 1MB
                    logger.warning(f"Downloaded MP4 is very small ({self.human_size(downloaded)}), might be invalid")
                    if attempt < max_attempts - 1:
                        os.remove(filepath)
                        continue
                
                return filepath
                
            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout error (attempt {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    continue
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error (attempt {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    continue
                    
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error {e.response.status_code} (attempt {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    continue
                    
            except Exception as e:
                error_msg = str(e)
                if "IncompleteRead" in error_msg:
                    logger.warning(f"Connection broken during download (attempt {attempt + 1}): {e}")
                    logger.info("This usually happens with large files through proxy. Will retry with resume capability.")
                else:
                    logger.error(f"Unexpected error (attempt {attempt + 1}): {e}")
                
                if attempt < max_attempts - 1:
                    # Clean up partial file if it's too small
                    if os.path.exists(filepath):
                        file_size = os.path.getsize(filepath)
                        if file_size < 1024 * 1024:  # Less than 1MB
                            os.remove(filepath)
                            logger.info("Removed incomplete small file")
                    continue
        
        logger.error(f"Failed to download {url} after {max_attempts} attempts")
        return None
    
    def human_size(self, n):
        """Convert bytes to human readable format"""
        for unit in ['B','KB','MB','GB','TB']:
            if n < 1024.0:
                return f"{n:3.1f}{unit}"
            n /= 1024.0
        return f"{n:.1f}PB"
    

    def check_file_size(self, download_url):
        """Check file size before downloading to avoid large files"""
        try:
            logger.info(f"🔍 Checking file size for: {download_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'video/mp4,video/*,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.eporner.com/',
            }
            
            # Use HEAD request to get file size without downloading
            response = self.session.head(download_url, headers=headers, timeout=15)
            
            if response.status_code in [200, 206, 302, 301]:
                # Get content length from headers
                content_length = response.headers.get('content-length')
                if content_length:
                    file_size = int(content_length)
                    file_size_mb = file_size / (1024 * 1024)
                    
                    logger.info(f"📏 File size: {self.human_size(file_size)}")
                    
                    if MAX_FILE_SIZE and file_size > MAX_FILE_SIZE:
                        logger.warning(f"⚠️  File too large ({self.human_size(file_size)} > {self.human_size(MAX_FILE_SIZE)}) - skipping")
                        return False, file_size
                    else:
                        logger.info(f"✅ File size OK ({self.human_size(file_size)})")
                        return True, file_size
                else:
                    logger.warning("⚠️  Could not determine file size - proceeding anyway")
                    return True, 0
            else:
                logger.warning(f"⚠️  Could not check file size (status: {response.status_code}) - proceeding anyway")
                return True, 0
                
        except Exception as e:
            logger.warning(f"⚠️  Error checking file size: {e} - proceeding anyway")
            return True, 0


    def test_network_connectivity(self):
        """Test network connectivity with current proxy"""
        logger.info("🔍 Testing network connectivity...")
        
        # Test proxy with eporner.com
        if USE_PROXY and self.current_proxy:
            self.test_proxy_with_eporner()
        else:
            logger.info("🚫 No proxy configured")
    
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
            video_data_list = self.db.get_video_data_for_download(limit=1000)  # Process up to 1000 videos
            
            if not video_data_list:
                logger.info("No videos to process")
                return
            
            logger.info(f"Found {len(video_data_list)} videos to process")
            
            # Set random proxy for this batch
            if USE_PROXY:
                if not self.set_random_proxy():
                    logger.error("❌ No proxies available, cannot proceed")
                    return
                
                # Test proxy with eporner.com
                self.test_network_connectivity()
                logger.info(f"🎯 Next proxy change in {self.next_proxy_change_at} videos")
            
            # Setup Telegram client
            client, bot_entity = await self.setup_telegram_client()
            if not client or not bot_entity:
                logger.error("Failed to setup Telegram client")
                return
            
            # Set up URL monitoring with proper tracking
            current_video_url = None
            current_diskwala_url = None
            
            @client.on(events.NewMessage(chats=bot_entity))
            async def url_handler(event):
                nonlocal current_diskwala_url
                if event.message and event.message.message:
                    text = event.message.message
                    for url in DISKWALA_PATTERN.findall(text):
                        current_diskwala_url = url
                        self.urls_found.append(url)
                        logger.info(f"🎉 DiskWala URL Found: {url}")
            
            logger.info("📡 Monitoring chat for DiskWala URLs...")
            
            # Process each video
            for idx, video_data in enumerate(video_data_list, start=1):
                try:
                    logger.info(f"\n[{idx}/{len(video_data_list)}] Processing video: {video_data['video_url']}")
                    
                    # Check if proxy should be rotated
                    if USE_PROXY and self.should_rotate_proxy():
                        self.rotate_proxy_if_needed()
                    
                    # Set current video URL for tracking
                    current_video_url = video_data['video_url']
                    current_diskwala_url = None  # Reset for each video
                    
                    # Check if video is already processed (any status)
                    is_processed, status, reason = self.db.is_video_processed(video_data['video_url'])
                    if is_processed:
                        logger.info(f"⏭️  Video already processed ({status}): {video_data['video_url']}")
                        if reason:
                            logger.info(f"   Reason: {reason}")
                        continue
                    
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
                    
                    # Get MP4 URL and modify quality from 480p to 240p
                    mp4_url = mp4_links[0]  # Get first MP4 link
                    
                    # Modify quality in URL for smaller file size
                    if MODIFY_QUALITY and f"/{QUALITY_FROM}/" in mp4_url:
                        original_url = mp4_url
                        # Replace both path and filename
                        mp4_url = mp4_url.replace(f"/{QUALITY_FROM}/", f"/{QUALITY_TO}/")
                        mp4_url = mp4_url.replace(f"-{QUALITY_FROM}p.mp4", f"-{QUALITY_TO}p.mp4")
                        av1_url = mp4_url.replace(f"-{QUALITY_FROM}p.mp4", f"-{QUALITY_TO}p-av1.mp4")
                        logger.info(f"🔄 Modified URL quality: {QUALITY_FROM}p → {QUALITY_TO}p")
                        logger.info(f"📥 Original URL: {original_url}")
                        logger.info(f"📥 Modified URL: {mp4_url}")
                        logger.info(f"📥 AV1 URL: {av1_url}")
                        mp4_url = av1_url
                    else:
                        logger.info(f"📥 Using original MP4 URL: {mp4_url}")
                    
                    # Check file size before downloading
                    file_size_ok, file_size = self.check_file_size(mp4_url)
                    if not file_size_ok:
                        file_size_mb = file_size / (1024 * 1024) if file_size > 0 else None
                        logger.warning(f"⏭️  Skipped MP4 download (file too large): {mp4_url}")
                        # Mark as processed with reason
                        self.db.mark_video_processed(
                            video_data['video_url'], 
                            'skipped', 
                            'File too large',
                            file_size_mb
                        )
                        continue
                    
                    # Download MP4 file
                    mp4_filename = f"video_{video_data['id']}.mp4"
                    mp4_path = self.download_file(mp4_url, mp4_filename)
                    
                    if not mp4_path:
                        logger.warning(f"⏭️  Skipped MP4 download (download failed): {mp4_url}")
                        # Mark as processed with reason
                        self.db.mark_video_processed(
                            video_data['video_url'], 
                            'skipped', 
                            'Download failed',
                            None
                        )
                        continue
                    
                    # Download JPG file
                    jpg_url = jpg_links[0]  # Get first JPG link
                    jpg_filename = f"image_{video_data['id']}.jpg"
                    jpg_path = self.download_file(jpg_url, jpg_filename)
                    
                    if not jpg_path:
                        logger.warning(f"⏭️  Skipped JPG download (likely too large): {jpg_url}")
                        # Clean up MP4 file if JPG download failed
                        self.cleanup_files(mp4_path, None)
                        # Mark as processed with reason
                        self.db.mark_video_processed(
                            video_data['video_url'], 
                            'skipped', 
                            'JPG download failed',
                            None
                        )
                        continue
                    
                    # Upload to DiskWala
                    upload_success = await self.upload_to_diskwala(client, bot_entity, mp4_path)
                    
                    if not upload_success:
                        logger.error(f"Failed to upload to DiskWala: {mp4_path}")
                        # Clean up files if upload failed
                        self.cleanup_files(mp4_path, jpg_path)
                        # Mark as processed with reason
                        self.db.mark_video_processed(
                            video_data['video_url'], 
                            'failed', 
                            'DiskWala upload failed',
                            None
                        )
                        continue
                    
                    # Wait until a DiskWala URL is received for THIS specific video
                    diskwala_timeout = 300      # seconds
                    diskwala_poll_interval = 2  # seconds
                    waited = 0
                    logger.info(f"⏳ Waiting for DiskWala URL for video: {video_data['video_url']}")
                    
                    while not current_diskwala_url and waited < diskwala_timeout:
                        await asyncio.sleep(diskwala_poll_interval)
                        waited += diskwala_poll_interval
                        
                        # Log progress every 30 seconds
                        if waited % 30 == 0:
                            logger.info(f"⏳ Still waiting for DiskWala URL... ({waited}s/{diskwala_timeout}s)")
                    
                    # Check if we got a DiskWala URL for this video
                    if current_diskwala_url:
                        diskwala_url = current_diskwala_url
                        logger.info(f"✅ Received DiskWala URL for video {idx}: {diskwala_url}")
                        
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
                        
                        # Mark as successfully processed
                        file_size_mb = os.path.getsize(mp4_path) / (1024 * 1024) if os.path.exists(mp4_path) else None
                        self.db.mark_video_processed(
                            video_data['video_url'], 
                            'success', 
                            'Successfully uploaded and posted',
                            file_size_mb
                        )
                        
                        logger.info(f"✅ Successfully processed: {video_data['video_url']}")
                        
                        # Increment proxy usage counter
                        self.videos_processed_with_current_proxy += 1
                        
                        # Clean up downloaded files after successful upload and posting
                        self.cleanup_files(mp4_path, jpg_path)
                    else:
                        logger.warning(f"❌ No DiskWala URL received for video {idx}: {video_data['video_url']}")
                        # Clean up files even if DiskWala URL not received
                        self.cleanup_files(mp4_path, jpg_path)
                        # Mark as processed with reason
                        self.db.mark_video_processed(
                            video_data['video_url'], 
                            'failed', 
                            'No DiskWala URL received',
                            None
                        )
                    
                    # Reset for next video
                    current_diskwala_url = None
                    
                    # Delay between uploads
                    if idx < len(video_data_list):
                        await asyncio.sleep(DELAY_BETWEEN_UPLOADS)
                
                except Exception as e:
                    logger.error(f"Error processing video {video_data['video_url']}: {e}")
                    # Mark as processed with error reason
                    self.db.mark_video_processed(
                        video_data['video_url'], 
                        'error', 
                        f'Processing error: {str(e)}',
                        None
                    )
                    # Reset for next video even on error
                    current_diskwala_url = None
                    continue
            
            # Summary
            logger.info(f"\n📊 Summary:")
            logger.info(f"   Videos processed: {len(video_data_list)}")
            logger.info(f"   DiskWala URLs found: {len(self.urls_found)}")
            
            # Get processed videos statistics
            stats = self.db.get_processed_videos_stats()
            if stats:
                logger.info(f"\n📈 Processed Videos Statistics:")
                for status, data in stats.items():
                    logger.info(f"   {status.upper()}: {data['count']} videos")
                    if data['avg_file_size_mb'] > 0:
                        logger.info(f"      Average file size: {data['avg_file_size_mb']:.2f} MB")
            
            # Get proxy statistics
            if USE_PROXY and self.current_proxy:
                proxy_stats = self.db.get_proxy_stats()
                if proxy_stats:
                    logger.info(f"\n🌐 Proxy Statistics:")
                    logger.info(f"   Total proxies: {proxy_stats.get('total', 0)}")
                    logger.info(f"   Active proxies: {proxy_stats.get('active', 0)}")
                    logger.info(f"   Current proxy: {self.current_proxy['ip']}:{self.current_proxy['port']} ({self.current_proxy['country']})")
                    logger.info(f"   Videos processed with current proxy: {self.videos_processed_with_current_proxy}")
                    logger.info(f"   Next proxy change in: {self.next_proxy_change_at - self.videos_processed_with_current_proxy} videos")
            
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
