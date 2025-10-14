import os
import time
import requests
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import json

def setup_chrome_driver():
    """Setup Chrome WebDriver with proper options for video downloading"""
    chrome_options = Options()
    
    # Basic options
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-javascript")  # We don't need JS for direct MP4 downloads
    
    # User agent
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Download preferences
    prefs = {
        "download.default_directory": os.path.abspath("downloads"),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.notifications": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Headless mode (comment out if you want to see the browser)
    chrome_options.add_argument("--headless")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except WebDriverException as e:
        print(f"Failed to create Chrome driver: {e}")
        print("Make sure ChromeDriver is installed and in PATH")
        return None

def extract_video_id(url):
    """Extract video ID from eporner URL to identify unique videos"""
    try:
        # For URLs like: https://www.eporner.com/dload/IL7JTGmJ6Jo/480/14284865-480p.mp4
        # Extract the video ID part (IL7JTGmJ6Jo)
        parts = url.split('/')
        if 'dload' in parts:
            dload_index = parts.index('dload')
            if dload_index + 1 < len(parts):
                return parts[dload_index + 1]
        return None
    except:
        return None

def check_video_already_downloaded(video_id, download_dir):
    """Check if a video with the same ID is already downloaded"""
    if not video_id:
        return False, None
    
    # Look for files that contain the video ID in their name
    for filename in os.listdir(download_dir):
        if video_id in filename and filename.endswith('.mp4'):
            return True, filename
    
    return False, None

def get_downloaded_files_before_download(download_dir):
    """Get list of files in download directory before starting download"""
    try:
        return set(os.listdir(download_dir))
    except:
        return set()

def find_new_downloaded_file(initial_files, download_dir, video_id):
    """Find the newly downloaded file after download attempt"""
    try:
        current_files = set(os.listdir(download_dir))
        new_files = current_files - initial_files
        
        # Look for new MP4 files
        for filename in new_files:
            if filename.endswith('.mp4'):
                # Check if it contains the video ID or is a reasonable size (> 1MB)
                file_path = os.path.join(download_dir, filename)
                file_size = os.path.getsize(file_path)
                
                if video_id and video_id in filename:
                    return filename, file_size
                elif file_size > 1024 * 1024:  # > 1MB, likely a real video
                    return filename, file_size
        
        return None, 0
    except:
        return None, 0

def download_mp4_files():
    """Download MP4 files from extracted_links.txt using Selenium browser automation"""
    
    # Read the extracted links file
    try:
        with open("extracted_links.txt", "r", encoding="utf-8") as f:
            links = f.read().strip().split("\n")
    except FileNotFoundError:
        print("extracted_links.txt not found. Please run run3.py first to generate links.")
        return
    
    # Filter MP4 links
    mp4_links = [link for link in links if link.endswith('.mp4')]
    
    if not mp4_links:
        print("No MP4 links found in extracted_links.txt")
        return
    
    print(f"Found {len(mp4_links)} MP4 links to download:")
    for i, link in enumerate(mp4_links, 1):
        print(f"{i}. {link}")
    
    # Create downloads directory if it doesn't exist
    download_dir = "downloads"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    # Track downloaded video IDs to prevent duplicates
    downloaded_video_ids = set()
    
    # Setup Chrome driver
    print("Setting up Chrome WebDriver...")
    driver = setup_chrome_driver()
    if not driver:
        print("Failed to setup Chrome driver. Exiting...")
        return
    
    try:
        # Set page load timeout
        driver.set_page_load_timeout(30)
        
        # First, visit eporner.com to establish session
        print("Establishing session with eporner.com...")
        driver.get("https://www.eporner.com/")
        time.sleep(3)  # Wait for page to load
        
        print("Session established successfully!")
        
    except Exception as e:
        print(f"Failed to establish session: {e}")
        driver.quit()
        return
    
    # Download each MP4 file using Selenium
    for i, url in enumerate(mp4_links, 1):
        max_retries = 3
        retry_count = 0
        success = False
        
        # Extract video ID to check for duplicates
        video_id = extract_video_id(url)
        
        # Check if this video is already downloaded
        already_downloaded, existing_filename = check_video_already_downloaded(video_id, download_dir)
        if already_downloaded:
            print(f"\n[{i}/{len(mp4_links)}] Video already downloaded: {existing_filename}")
            print(f"   Video ID: {video_id}")
            print(f"   Skipping URL: {url}")
            continue
        
        # Add to tracking set to prevent downloading same video multiple times in this session
        if video_id:
            downloaded_video_ids.add(video_id)
        
        while retry_count < max_retries and not success:
            try:
                if retry_count > 0:
                    print(f"\n[{i}/{len(mp4_links)}] Retry {retry_count}/{max_retries-1} for: {url}")
                    time.sleep(2 ** retry_count)  # Exponential backoff: 2, 4, 8 seconds
                else:
                    print(f"\n[{i}/{len(mp4_links)}] Downloading: {url}")
                    if video_id:
                        print(f"   Video ID: {video_id}")
                
                # Get filename from URL for checking if file exists
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path)
                if not filename.endswith('.mp4'):
                    filename = f"video_{i}.mp4"
                
                filepath = os.path.join(download_dir, filename)
                
                # Double-check if file already exists (in case it was downloaded between checks)
                if os.path.exists(filepath):
                    print(f"File already exists: {filename}")
                    success = True
                    continue
                
                # Use Selenium to download the MP4 file
                print("Using Selenium browser automation for download...")
                
                # Get list of files before download
                files_before = get_downloaded_files_before_download(download_dir)
                
                # Navigate to the MP4 URL
                driver.get(url)
                
                # Wait for download to start and complete
                print("Waiting for download to start...")
                download_started = False
                downloaded_filename = None
                downloaded_size = 0
                
                # Monitor for new files for up to 30 seconds
                for attempt in range(30):
                    time.sleep(1)
                    
                    # Check for new files
                    new_filename, new_size = find_new_downloaded_file(files_before, download_dir, video_id)
                    
                    if new_filename and new_size > 0:
                        download_started = True
                        downloaded_filename = new_filename
                        downloaded_size = new_size
                        print(f"✓ Download detected: {downloaded_filename} ({downloaded_size:,} bytes)")
                        break
                
                if download_started:
                    # Wait for download to complete by monitoring file size
                    print("Waiting for download to complete...")
                    initial_size = downloaded_size
                    stable_count = 0
                    
                    while True:
                        try:
                            current_size = os.path.getsize(os.path.join(download_dir, downloaded_filename))
                            if current_size == initial_size:
                                stable_count += 1
                                if stable_count >= 3:  # File size stable for 3 seconds
                                    break
                            else:
                                stable_count = 0
                                initial_size = current_size
                        except:
                            pass
                        time.sleep(1)
                    
                    final_size = os.path.getsize(os.path.join(download_dir, downloaded_filename))
                    print(f"✓ Download completed: {downloaded_filename} ({final_size:,} bytes)")
                    success = True
                else:
                    print(f"✗ Download failed to start for: {url}")
                    raise Exception("Download did not start")
                
                # Small delay between downloads
                time.sleep(2)
                
            except TimeoutException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ Timeout after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ Timeout, retrying... ({retry_count}/{max_retries-1})")
                    
            except WebDriverException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ WebDriver error after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ WebDriver error, retrying... ({retry_count}/{max_retries-1})")
                    
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ Unexpected error after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ Unexpected error, retrying... ({retry_count}/{max_retries-1})")
    
    # Close the browser
    print("Closing browser...")
    driver.quit()
    
    # Count total files in downloads directory
    total_files = len([f for f in os.listdir(download_dir) if f.endswith('.mp4')])
    
    print(f"\nDownload completed! Files saved in '{download_dir}' directory.")
    print(f"Total MP4 files in downloads directory: {total_files}")
    print(f"Unique video IDs processed: {len(downloaded_video_ids)}")

if __name__ == "__main__":
    print("Starting MP4 download bot with Selenium browser automation...")
    download_mp4_files()
