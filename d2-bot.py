import os
import yt_dlp
import time
from urllib.parse import urlparse

def download_mp4_files():
    """Download MP4 files from extracted_links.txt using yt-dlp"""
    
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
    
    # yt-dlp configuration
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
                
            except yt_dlp.DownloadError as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"\n✗ Download failed after {max_retries} retries: {url}")
                    print(f"   Error: {e}")
                else:
                    print(f"\n⚠ Download error, retrying... ({retry_count}/{max_retries-1})")
                    
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
