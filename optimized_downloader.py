import requests
import os
from urllib.parse import urlparse
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- PROXY CONFIGURATION ---
# This is the most important part you were missing
PROXY_IP = "191.96.254.130"
PROXY_PORT = "6177"
PROXY_USERNAME = "tjijutki"
PROXY_PASSWORD = "4vg93ifc50gnx"

def download_mp4_files():
    """Download MP4 files from extracted_links.txt using a residential proxy."""
    
    try:
        with open("extracted_links.txt", "r", encoding="utf-8") as f:
            links = f.read().strip().split("\n")
    except FileNotFoundError:
        print("extracted_links.txt not found.")
        return
    
    mp4_links = [link for link in links if link.endswith('.mp4')]
    
    if not mp4_links:
        print("No MP4 links found in extracted_links.txt")
        return
    
    print(f"Found {len(mp4_links)} MP4 links to download:")
    
    download_dir = "downloads"
    os.makedirs(download_dir, exist_ok=True)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.eporner.com/",
    }
    
    # **FIX 1: Configure the proxy**
    proxies = {
        'http': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_IP}:{PROXY_PORT}',
        'https': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_IP}:{PROXY_PORT}'
    }

    # Create session
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set headers and proxies for the entire session
    session.headers.update(headers)
    session.proxies.update(proxies)
    
    for i, url in enumerate(mp4_links, 1):
        try:
            print(f"\n[{i}/{len(mp4_links)}] Downloading: {url}")
            
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            filepath = os.path.join(download_dir, filename)
            
            if os.path.exists(filepath):
                print(f"File already exists: {filename}")
                continue
            
            # Use the session to download the file
            response = session.get(url, stream=True, timeout=60) # Increased timeout
            response.raise_for_status()

            # **FIX 2: Validate the content before downloading**
            content_type = response.headers.get('content-type', '').lower()
            if 'video' not in content_type:
                print(f"\n✗ Failed: Server did not return a video file. Content-Type is '{content_type}'. Skipping.")
                # This happens when you get an HTML page instead of the video.
                continue

            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\rProgress: {progress:.1f}%", end="", flush=True)
            
            print(f"\n✓ Downloaded: {filename}")
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f"\n✗ Request failed for {url}: {e}")
            
    print(f"\nDownload completed! Files saved in '{download_dir}' directory.")

if __name__ == "__main__":
    print("Starting MP4 download bot...")
    download_mp4_files()