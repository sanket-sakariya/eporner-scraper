import os
import yt_dlp
import requests
import time
from urllib.parse import urlparse

def download_mp4_files():
    """Download MP4 files from extracted_links.txt using yt-dlp for video pages and requests for direct MP4 links"""
    
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
    
    # Headers for direct MP4 downloads
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.eporner.com/",
    }
    
    # Create session with retry strategy for direct downloads
    session = requests.Session()
    session.headers.update(headers)
    
    # yt-dlp configuration for video pages (not direct MP4 links)
    ydl_opts = {
        'outtmpl': os.path.join(download_dir, '%(title)s.%(ext)s'),
        'format': 'best[ext=mp4]/best',
        'noplaylist': True,
        'extract_flat': False,
        'writethumbnail': False,
        'writeinfojson': False,
        'writesubtitles': False,
        'writeautomaticsub': False,
        'ignoreerrors': True,
        'no_warnings': False,
        'extractaudio': False,
        'audioformat': 'mp3',
        'embed_subs': False,
        'writesubtitles': False,
        'writeautomaticsub': False,
        'postprocessors': [],
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'referer': 'https://www.eporner.com/',
        'headers': {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
        }
    }
    
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
                
                # Get filename from URL for checking if file exists
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
                
                # Check if it's a direct MP4 link or a video page URL
                if url.endswith('.mp4') and '/dload/' in url:
                    # This is a direct MP4 download link - use requests
                    print("Using direct download method for MP4 link...")
                    
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
                    
                else:
                    # This is a video page URL - use yt-dlp
                    print("Using yt-dlp for video page...")
                    
                    # Create yt-dlp instance with progress hook
                    def progress_hook(d):
                        if d['status'] == 'downloading':
                            if 'total_bytes' in d:
                                percent = (d['downloaded_bytes'] / d['total_bytes']) * 100
                                print(f"\rProgress: {percent:.1f}% ({d['downloaded_bytes']}/{d['total_bytes']} bytes)", end="", flush=True)
                            elif 'total_bytes_estimate' in d:
                                percent = (d['downloaded_bytes'] / d['total_bytes_estimate']) * 100
                                print(f"\rProgress: {percent:.1f}% (estimated)", end="", flush=True)
                        elif d['status'] == 'finished':
                            print(f"\n✓ Downloaded: {d['filename']}")
                    
                    ydl_opts['progress_hooks'] = [progress_hook]
                    
                    # Download using yt-dlp
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([url])
                
                success = True
                
                # Small delay between downloads
                time.sleep(2)
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ Request failed after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ Request error, retrying... ({retry_count}/{max_retries-1})")
                    
            except yt_dlp.DownloadError as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ yt-dlp download failed after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ yt-dlp download error, retrying... ({retry_count}/{max_retries-1})")
                    
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ Unexpected error after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ Unexpected error, retrying... ({retry_count}/{max_retries-1})")
    
    print(f"\nDownload completed! Files saved in '{download_dir}' directory.")

if __name__ == "__main__":
    print("Starting MP4 download bot with yt-dlp...")
    download_mp4_files()
