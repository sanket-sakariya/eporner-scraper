import os
import time
import requests
from urllib.parse import urlparse
import json

def setup_session():
    """Setup requests session with proper cookies and headers for eporner downloads"""
    session = requests.Session()
    
    # Cookies that work for eporner downloads
    cookies = {
        'PHPSESSID': '3aead98c24739b738100ece4e0d858fe',
    }
    
    # Headers that mimic a real browser request
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,id;q=0.8',
        'priority': 'u=0, i',
        'referer': 'https://www.eporner.com/',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-site',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    }
    
    session.cookies.update(cookies)
    session.headers.update(headers)
    
    return session

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

def get_cdn_url_from_page(session, video_page_url):
    """Extract the actual CDN URL from the video page"""
    try:
        print(f"Visiting video page: {video_page_url}")
        response = session.get(video_page_url, timeout=30)
        response.raise_for_status()
        
        # Look for CDN URLs in the page content
        import re
        
        # Pattern to match CDN URLs like: https://vid-s1-c50-fr-cdn.eporner.com/...
        cdn_pattern = r'https://vid-[^"]*\.eporner\.com/[^"]*\.mp4[^"]*'
        cdn_matches = re.findall(cdn_pattern, response.text)
        
        if cdn_matches:
            # Return the first CDN URL found
            cdn_url = cdn_matches[0]
            print(f"Found CDN URL: {cdn_url}")
            return cdn_url
        
        # Alternative pattern for other CDN formats
        alt_pattern = r'https://[^"]*cdn[^"]*\.eporner\.com/[^"]*\.mp4[^"]*'
        alt_matches = re.findall(alt_pattern, response.text)
        
        if alt_matches:
            cdn_url = alt_matches[0]
            print(f"Found alternative CDN URL: {cdn_url}")
            return cdn_url
        
        print("No CDN URL found in page content")
        return None
        
    except Exception as e:
        print(f"Error extracting CDN URL: {e}")
        return None

def download_video_direct(session, url, download_dir, video_id=None):
    """Download video using direct requests approach"""
    try:
        # Check if this is a dload URL that needs CDN extraction
        if '/dload/' in url:
            # Extract video ID and construct video page URL
            if video_id:
                video_page_url = f"https://www.eporner.com/video/{video_id}/"
            else:
                # Try to extract video ID from dload URL
                parts = url.split('/')
                if 'dload' in parts:
                    dload_index = parts.index('dload')
                    if dload_index + 1 < len(parts):
                        video_id = parts[dload_index + 1]
                        video_page_url = f"https://www.eporner.com/video/{video_id}/"
                    else:
                        print("Could not extract video ID from dload URL")
                        return False, None, 0
                else:
                    print("Invalid dload URL format")
                    return False, None, 0
            
            # Get the actual CDN URL from the video page
            cdn_url = get_cdn_url_from_page(session, video_page_url)
            if not cdn_url:
                print("Could not extract CDN URL from video page")
                return False, None, 0
            
            # Use the CDN URL for download
            url = cdn_url
        
        # Get filename from URL
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        
        # Clean filename - remove query parameters and ensure .mp4 extension
        if '?' in filename:
            filename = filename.split('?')[0]
        if not filename.endswith('.mp4'):
            filename = f"video_{video_id or 'unknown'}.mp4"
        
        # Remove any problematic characters from filename
        filename = "".join(c for c in filename if c.isalnum() or c in "._-")
        
        filepath = os.path.join(download_dir, filename)
        
        # Check if file already exists
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath)
            if file_size > 1024 * 1024:  # > 1MB, likely a real video
                print(f"File already exists: {filename} ({file_size:,} bytes)")
                return True, filename, file_size
        
        print(f"Downloading: {filename}")
        print(f"URL: {url}")
        
        # Make the request
        response = session.get(url, stream=True, timeout=30, allow_redirects=True)
        response.raise_for_status()
        
        # Check content type
        content_type = response.headers.get('content-type', '').lower()
        if 'text/html' in content_type or 'application/json' in content_type:
            print(f"Received HTML/JSON instead of video (Content-Type: {content_type})")
            return False, None, 0
        
        # Get file size for progress tracking
        total_size = int(response.headers.get('content-length', 0))
        
        # Download the file
        downloaded = 0
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Show progress
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\rProgress: {progress:.1f}% ({downloaded:,}/{total_size:,} bytes)", end="", flush=True)
        
        print(f"\n✓ Downloaded: {filename} ({downloaded:,} bytes)")
        return True, filename, downloaded
        
    except requests.exceptions.RequestException as e:
        print(f"\n✗ Download failed: {e}")
        return False, None, 0
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        return False, None, 0

def download_mp4_files_optimized():
    """Download MP4 files using optimized direct requests approach"""
    
    # Read the extracted links file
    try:
        with open("extracted_links.txt", "r", encoding="utf-8") as f:
            links = f.read().strip().split("\n")
    except FileNotFoundError:
        print("extracted_links.txt not found. Please run the scraper first to generate links.")
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
    
    # Setup session
    print("\nSetting up optimized download session...")
    session = setup_session()
    
    # Track downloaded video IDs to prevent duplicates
    downloaded_video_ids = set()
    successful_downloads = 0
    failed_downloads = 0
    
    # Download each MP4 file
    for i, url in enumerate(mp4_links, 1):
        print(f"\n[{i}/{len(mp4_links)}] Processing: {url}")
        
        # Extract video ID to check for duplicates
        video_id = extract_video_id(url)
        
        # Check if this video is already downloaded
        already_downloaded, existing_filename = check_video_already_downloaded(video_id, download_dir)
        if already_downloaded:
            print(f"Video already downloaded: {existing_filename}")
            print(f"Video ID: {video_id}")
            continue
        
        # Add to tracking set to prevent downloading same video multiple times in this session
        if video_id:
            downloaded_video_ids.add(video_id)
        
        # Download the video
        success, filename, file_size = download_video_direct(session, url, download_dir, video_id)
        
        if success:
            successful_downloads += 1
        else:
            failed_downloads += 1
        
        # Small delay between downloads to be respectful
        time.sleep(1)
    
    # Count total files in downloads directory
    total_files = len([f for f in os.listdir(download_dir) if f.endswith('.mp4')])
    
    print(f"\n{'='*60}")
    print(f"Download Summary:")
    print(f"Total MP4 files in downloads directory: {total_files}")
    print(f"Successful downloads this session: {successful_downloads}")
    print(f"Failed downloads this session: {failed_downloads}")
    print(f"Unique video IDs processed: {len(downloaded_video_ids)}")
    print(f"{'='*60}")

if __name__ == "__main__":
    print("Starting optimized MP4 downloader...")
    print("Using direct requests approach with cookies and headers")
    download_mp4_files_optimized()
