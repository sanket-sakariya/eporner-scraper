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
    
    # Server-specific options
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-field-trial-config")
    chrome_options.add_argument("--disable-back-forward-cache")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    chrome_options.add_argument("--disable-hang-monitor")
    chrome_options.add_argument("--disable-prompt-on-repost")
    chrome_options.add_argument("--disable-sync")
    chrome_options.add_argument("--disable-translate")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--disable-default-apps")
    chrome_options.add_argument("--disable-background-networking")
    chrome_options.add_argument("--disable-component-extensions-with-background-pages")
    chrome_options.add_argument("--disable-client-side-phishing-detection")
    chrome_options.add_argument("--disable-component-update")
    chrome_options.add_argument("--disable-domain-reliability")
    chrome_options.add_argument("--disable-features=TranslateUI")
    chrome_options.add_argument("--disable-features=BlinkGenPropertyTrees")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--disable-javascript")  # We don't need JS for direct MP4 downloads
    
    # User agent
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Download preferences
    prefs = {
        "download.default_directory": os.path.abspath("downloads"),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "safebrowsing.disable_download_protection": True,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.media_stream": 2,
        "profile.default_content_setting_values.geolocation": 2,
        "profile.default_content_setting_values.camera": 2,
        "profile.default_content_setting_values.microphone": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Headless mode for server
    chrome_options.add_argument("--headless=new")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
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
        driver.set_page_load_timeout(60)
        
        # First, visit eporner.com to establish session
        print("Establishing session with eporner.com...")
        driver.get("https://www.eporner.com/")
        time.sleep(5)  # Wait for page to load
        
        # Try to interact with the page to establish proper session
        try:
            # Look for and click any "Accept" or "Continue" buttons
            accept_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Continue') or contains(text(), 'OK')]")
            for button in accept_buttons:
                try:
                    button.click()
                    time.sleep(2)
                except:
                    pass
        except:
            pass
        
        # Navigate to a video page to establish proper session
        print("Establishing video session...")
        driver.get("https://www.eporner.com/video/IL7JTGmJ6Jo/friends-enjoying-with-indian-girlfriend-in-a-hotel-room/")
        time.sleep(5)
        
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
                
                # Try Selenium first, fallback to requests if it fails
                selenium_success = False
                
                try:
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
                        selenium_success = True
                        success = True
                    else:
                        print(f"✗ Selenium download failed to start for: {url}")
                        raise Exception("Selenium download did not start")
                        
                except Exception as selenium_error:
                    print(f"Selenium failed: {selenium_error}")
                    print("Trying fallback method with requests...")
                    
                    # Fallback to requests method with cookies from Selenium
                    try:
                        # Get cookies from Selenium session
                        selenium_cookies = driver.get_cookies()
                        cookie_dict = {}
                        for cookie in selenium_cookies:
                            cookie_dict[cookie['name']] = cookie['value']
                        
                        headers = {
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                            "Accept": "video/mp4,video/*,*/*;q=0.9",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Accept-Encoding": "identity",
                            "Connection": "keep-alive",
                            "Referer": "https://www.eporner.com/video/IL7JTGmJ6Jo/friends-enjoying-with-indian-girlfriend-in-a-hotel-room/",
                            "Sec-Fetch-Dest": "video",
                            "Sec-Fetch-Mode": "no-cors",
                            "Sec-Fetch-Site": "same-origin",
                            "Origin": "https://www.eporner.com",
                        }
                        
                        # Create session with cookies
                        session = requests.Session()
                        session.headers.update(headers)
                        
                        # Add cookies to session
                        for name, value in cookie_dict.items():
                            session.cookies.set(name, value)
                        
                        print("Attempting download with session cookies...")
                        response = session.get(url, stream=True, timeout=30, allow_redirects=True)
                        response.raise_for_status()
                        
                        # Check if we got HTML content instead of video
                        content_type = response.headers.get('content-type', '').lower()
                        if 'text/html' in content_type or 'application/json' in content_type:
                            print("Received HTML/JSON instead of video")
                            print(f"Content-Type: {content_type}")
                            print(f"Response preview: {response.text[:200]}...")
                            raise Exception("Got HTML content")
                        
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
                        
                        print(f"\n✓ Downloaded via requests: {filename}")
                        success = True
                        
                    except Exception as requests_error:
                        print(f"Requests fallback also failed: {requests_error}")
                        
                        # Try one more method - direct wget/curl approach
                        print("Trying final fallback method...")
                        try:
                            import subprocess
                            import shutil
                            
                            # Check if wget is available
                            if shutil.which("wget"):
                                print("Using wget for download...")
                                cmd = [
                                    "wget",
                                    "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                                    "--referer=https://www.eporner.com/",
                                    "--header=Accept: video/mp4,video/*,*/*;q=0.9",
                                    "--header=Accept-Language: en-US,en;q=0.9",
                                    "--header=Connection: keep-alive",
                                    "-O", filepath,
                                    url
                                ]
                                
                                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                                if result.returncode == 0 and os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
                                    print(f"✓ Downloaded via wget: {filename}")
                                    success = True
                                else:
                                    print(f"wget failed: {result.stderr}")
                                    raise Exception("wget download failed")
                            else:
                                raise Exception("wget not available")
                                
                        except Exception as wget_error:
                            print(f"Final fallback also failed: {wget_error}")
                            raise Exception("All download methods failed")
                
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
