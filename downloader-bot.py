import requests
import os
from urllib.parse import urlparse
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def download_mp4_files():
    """Download MP4 files from extracted_links.txt"""
    
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
    
    # Headers for requests
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.eporner.com/",
    }
    
    # Create session with retry strategy
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,  # Total number of retries
        backoff_factor=2,  # Wait time between retries: 2, 4, 8 seconds
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry
        allowed_methods=["HEAD", "GET", "OPTIONS"]  # Methods to retry
    )
    
    # Mount adapter with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set headers for the session
    session.headers.update(headers)
    
    # Download each MP4 file
    for i, url in enumerate(mp4_links, 1):
        max_retries = 3
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                if retry_count > 0:
                    print(f"\n[{i}/{len(mp4_links)}] Retry {retry_count}/{max_retries-1} for: {url}")
                    time.sleep(2 ** retry_count)  # Exponential backoff: 2, 4, 8 seconds
                else:
                    print(f"\n[{i}/{len(mp4_links)}] Downloading: {url}")
                
                # Get filename from URL
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path)
                if not filename.endswith('.mp4'):
                    filename = f"video_{i}.mp4"
                
                filepath = os.path.join(download_dir, filename)
                
                # Check if file already exists
                if os.path.exists(filepath):
                    print(f"File already exists: {filename}")
                    success = True
                    continue
                
                # Download the file with session
                response = session.get(url, stream=True, timeout=30)
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
                success = True
                
                # Small delay between downloads
                time.sleep(2)
                
            except requests.exceptions.ConnectionError as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ Connection failed after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ Connection error, retrying... ({retry_count}/{max_retries-1})")
                    
            except requests.exceptions.Timeout as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ Timeout after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ Timeout, retrying... ({retry_count}/{max_retries-1})")
                    
            except requests.exceptions.RequestException as e:
                print(f"\n✗ Request failed: {url}")
                print(f"   Error: {e}")
                break  # Don't retry for other request exceptions
                
            except Exception as e:
                print(f"\n✗ Unexpected error downloading {url}: {e}")
                break  # Don't retry for unexpected errors
    
    print(f"\nDownload completed! Files saved in '{download_dir}' directory.")

if __name__ == "__main__":
    print("Starting MP4 download bot...")
    download_mp4_files()
