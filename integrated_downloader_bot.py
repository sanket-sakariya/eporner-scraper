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
PROXY_IP = "191.96.254.130"
PROXY_PORT = "6177"
PROXY_USERNAME = "tjijutki"
PROXY_PASSWORD = "4vg93ifc50gnx"
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

# -------------------- CLASSES --------------------

class IntegratedDownloaderBot:
    def __init__(self):
        self.db = DatabaseManager()
        self.download_dir = DOWNLOAD_DIR
        self.urls_found = []
        
        # Create download directory
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
        
        # Setup requests session with server-optimized settings
        self.session = requests.Session()
        
        # Configure proxy if enabled
        if USE_PROXY:
            self.proxies = {
                'http': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_IP}:{PROXY_PORT}',
                'https': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_IP}:{PROXY_PORT}'
            }
            self.session.proxies.update(self.proxies)
            logger.info(f"🔗 Proxy configured: {PROXY_IP}:{PROXY_PORT}")
        else:
            self.proxies = None
            logger.info("🚫 Proxy disabled")
        
        # Log file size limit
        if MAX_FILE_SIZE:
            logger.info(f"📏 File size limit: {self.human_size(MAX_FILE_SIZE)}")
        else:
            logger.info("📏 File size limit: None (no limit)")
        
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
                    # Add Range header for resume
                    current_headers['Range'] = f'bytes={downloaded}-'
                    response = self.session.get(url, headers=current_headers, stream=True, timeout=(30, 300))
                
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
    
    def test_proxy_connection(self):
        """Test proxy connection if enabled"""
        if not USE_PROXY:
            logger.info("🚫 Proxy testing skipped (proxy disabled)")
            return True
            
        logger.info(f"🔍 Testing proxy connection: {PROXY_IP}:{PROXY_PORT}")
        
        test_urls = [
            "http://httpbin.org/ip",
            "https://httpbin.org/ip",
            "http://ipinfo.io/ip",
            "https://api.ipify.org",
        ]
        
        for url in test_urls:
            try:
                logger.info(f"Testing proxy with: {url}")
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    logger.info(f"✅ Proxy connection successful via {url}")
                    logger.info(f"   Response: {response.text.strip()}")
                    return True
                elif response.status_code == 503:
                    logger.warning(f"⚠️  Server overloaded (503) - trying next endpoint...")
                    continue
                elif response.status_code == 407:
                    logger.error(f"❌ Proxy authentication failed (407) - check credentials")
                    return False
                else:
                    logger.warning(f"⚠️  Proxy test failed with status: {response.status_code} - trying next endpoint...")
                    continue
                    
            except requests.exceptions.ProxyError as e:
                logger.warning(f"⚠️  Proxy error with {url}: {e}")
                continue
            except requests.exceptions.Timeout:
                logger.warning(f"⚠️  Timeout with {url} - trying next endpoint...")
                continue
            except Exception as e:
                logger.warning(f"⚠️  Error testing {url}: {e}")
                continue
        
        logger.warning("⚠️  All proxy test endpoints failed, but proceeding anyway...")
        return True  # Proceed anyway as proxy might work for actual downloads

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

    def test_proxy_with_download_url(self, download_url):
        """Test proxy specifically with a download URL"""
        if not USE_PROXY:
            return True
            
        try:
            logger.info(f"🔍 Testing proxy with download URL...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'video/mp4,video/*,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.eporner.com/',
            }
            
            # Test with HEAD request first (faster)
            response = self.session.head(download_url, headers=headers, timeout=15)
            
            if response.status_code in [200, 206, 302, 301]:
                logger.info(f"✅ Proxy works with download URL (status: {response.status_code})")
                return True
            elif response.status_code == 503:
                logger.warning(f"⚠️  Download server overloaded (503) - proxy may still work")
                return True  # Allow to proceed as proxy might work for actual download
            else:
                logger.warning(f"⚠️  Download URL test failed with status: {response.status_code}")
                return False
                
        except Exception as e:
            logger.warning(f"⚠️  Download URL proxy test failed: {e}")
            return False

    def test_network_connectivity(self):
        """Test network connectivity and diagnose server issues"""
        logger.info("🔍 Testing network connectivity...")
        
        # Test proxy first if enabled
        if USE_PROXY:
            self.test_proxy_connection()
        
        test_urls = [
            "https://www.eporner.com/",
            "https://httpbin.org/ip",
            "https://www.google.com/",
        ]
        
        for url in test_urls:
            try:
                logger.info(f"Testing: {url}")
                response = self.session.get(url, timeout=10)
                logger.info(f"✅ {url} - Status: {response.status_code}")
                
                if "eporner.com" in url:
                    # Check if we're being blocked
                    if response.status_code == 403:
                        logger.warning("🚫 eporner.com is blocking our requests (403 Forbidden)")
                    elif "blocked" in response.text.lower() or "access denied" in response.text.lower():
                        logger.warning("🚫 eporner.com is blocking our access")
                    else:
                        logger.info("✅ eporner.com is accessible")
                        
            except Exception as e:
                logger.error(f"❌ {url} - Error: {e}")
        
        # Test DNS resolution
        try:
            import socket
            ip = socket.gethostbyname("www.eporner.com")
            logger.info(f"✅ DNS resolution for eporner.com: {ip}")
        except Exception as e:
            logger.error(f"❌ DNS resolution failed: {e}")
    
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
            # Test network connectivity and proxy first
            self.test_network_connectivity()
            
            # Get video data from database
            video_data_list = self.db.get_video_data_for_download(limit=1)
            
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
                    
                    # Check file size first to avoid large downloads
                    mp4_url = mp4_links[0]  # Get first MP4 link
                    size_ok, file_size = self.check_file_size(mp4_url)
                    
                    if not size_ok:
                        logger.warning(f"⏭️  Skipping video due to large file size: {video_data['video_url']}")
                        continue
                    
                    # Test proxy with download URL
                    if USE_PROXY:
                        logger.info(f"🔍 Testing proxy with MP4 URL: {mp4_url}")
                        proxy_test_result = self.test_proxy_with_download_url(mp4_url)
                        if not proxy_test_result:
                            logger.warning(f"⚠️  Proxy test failed for MP4 URL, but proceeding anyway...")
                    
                    # Download MP4 file
                    mp4_filename = f"video_{video_data['id']}.mp4"
                    mp4_path = self.download_file(mp4_url, mp4_filename)
                    
                    if not mp4_path:
                        logger.error(f"Failed to download MP4: {mp4_url}")
                        continue
                    
                    # Test proxy with JPG URL and download JPG file
                    jpg_url = jpg_links[0]  # Get first JPG link
                    if USE_PROXY:
                        logger.info(f"🔍 Testing proxy with JPG URL: {jpg_url}")
                        proxy_test_result = self.test_proxy_with_download_url(jpg_url)
                        if not proxy_test_result:
                            logger.warning(f"⚠️  Proxy test failed for JPG URL, but proceeding anyway...")
                    
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
                    await asyncio.sleep(60)
                    
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
