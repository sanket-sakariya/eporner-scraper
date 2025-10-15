"""
Download an .mp4 file from eporner using undetected-chromedriver with residential proxy.
- Uses undetected-chromedriver for better stealth and performance
- Supports residential proxy with authentication
- Faster and more reliable downloads
"""

# pip install undetected-chromedriver requests

import undetected_chromedriver as uc
import requests
import time
import os
import shutil
from pathlib import Path
import tempfile


# ---------------- CONFIG ----------------
VIDEO_PAGE_URL = "https://www.eporner.com/video-Ux4DafGxvAz/hidden-lust/"
DOWNLOAD_URL = "https://www.eporner.com/dload/Ux4DafGxvAz/240/14177153-240p-av1.mp4?click=1"
OUTPUT_FILENAME = "14177153-240p-av1.mp4"

# Proxy configuration from dev.txt
PROXY_IP = "191.96.254.130"
PROXY_PORT = "6177"
PROXY_USERNAME = "tjijutki"
PROXY_PASSWORD = "4vg93ifc50gnx"
# ----------------------------------------


def download_with_requests(download_url, output_filename, proxy_config):
    """Download file using requests with proxy support."""
    try:
        print(f"[+] Downloading with requests: {download_url}")
        
        # Setup proxy for requests
        proxies = {
            'http': f'http://{proxy_config["username"]}:{proxy_config["password"]}@{proxy_config["ip"]}:{proxy_config["port"]}',
            'https': f'http://{proxy_config["username"]}:{proxy_config["password"]}@{proxy_config["ip"]}:{proxy_config["port"]}'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'Accept': 'video/mp4,video/*,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.eporner.com/',
        }
        
        response = requests.get(download_url, proxies=proxies, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        # Get file size for progress tracking
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        
        with open(output_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    
                    # Show progress
                    if total_size > 0:
                        progress = (downloaded_size / total_size) * 100
                        print(f"\r[+] Download progress: {progress:.1f}% ({downloaded_size / (1024*1024):.1f}MB / {total_size / (1024*1024):.1f}MB)", end='', flush=True)
        
        print(f"\n[✓] Download completed: {output_filename}")
        return True
        
    except Exception as e:
        print(f"\n[!] Error downloading with requests: {e}")
        return False


def test_proxy_connection(proxy_config):
    """Test if proxy connection is working."""
    try:
        print(f"[+] Testing proxy connection: {proxy_config['ip']}:{proxy_config['port']}")
        
        proxies = {
            'http': f'http://{proxy_config["username"]}:{proxy_config["password"]}@{proxy_config["ip"]}:{proxy_config["port"]}',
            'https': f'http://{proxy_config["username"]}:{proxy_config["password"]}@{proxy_config["ip"]}:{proxy_config["port"]}'
        }
        
        response = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=10)
        if response.status_code == 200:
            print(f"[✓] Proxy connection successful")
            return True
        else:
            print(f"[!] Proxy test failed with status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"[!] Proxy connection test failed: {e}")
        return False


def main():
    print("[+] Starting eporner video downloader with residential proxy...")
    
    # Setup proxy configuration
    proxy_config = {
        "ip": PROXY_IP,
        "port": PROXY_PORT,
        "username": PROXY_USERNAME,
        "password": PROXY_PASSWORD
    }
    
    print(f"[+] Using residential proxy: {PROXY_IP}:{PROXY_PORT}")
    
    # Test proxy connection first
    if not test_proxy_connection(proxy_config):
        print("[!] Proxy connection test failed. Please check your proxy settings.")
        return False
    
    # Setup output file path
    output_path = os.path.join(os.getcwd(), OUTPUT_FILENAME)
    
    # Try downloading with requests (faster and more reliable)
    print(f"[+] Attempting direct download with requests...")
    if download_with_requests(DOWNLOAD_URL, output_path, proxy_config):
        # Verify file was downloaded
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            print(f"[✓] Video downloaded successfully: {output_path}")
            print(f"[+] File size: {file_size / (1024*1024):.2f} MB")
            return True
        else:
            print("[!] Download completed but file not found")
            return False
    else:
        print("[!] Direct download failed, trying with undetected-chromedriver...")
        return download_with_chrome(DOWNLOAD_URL, output_path, proxy_config)


def download_with_chrome(download_url, output_filename, proxy_config):
    """Fallback method using undetected-chromedriver."""
    driver = None
    temp_dir = None
    
    try:
        print(f"[+] Setting up undetected-chromedriver...")
        
        # Create temp directory for Chrome user data
        temp_dir = tempfile.mkdtemp(prefix="chrome_session_")
        
        # Setup Chrome options
        options = uc.ChromeOptions()
        options.add_argument(f"--user-data-dir={temp_dir}")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--headless=new")
        
        # Setup proxy
        proxy_server = f"{proxy_config['ip']}:{proxy_config['port']}"
        options.add_argument(f"--proxy-server=http://{proxy_server}")
        
        # Setup download preferences
        prefs = {
            "download.default_directory": os.path.dirname(output_filename),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
        }
        options.add_experimental_option("prefs", prefs)
        
        # Initialize driver
        driver = uc.Chrome(options=options)
        print(f"[+] Chrome driver started with proxy: {proxy_server}")
        
        # Navigate to download URL
        print(f"[+] Navigating to download URL...")
        driver.get(download_url)
        
        # Wait for download to start and complete
        time.sleep(5)  # Give it time to start
        
        # Check if download completed
        download_dir = os.path.dirname(output_filename)
        downloaded_files = [f for f in Path(download_dir).iterdir() if f.is_file() and f.suffix == '.mp4']
        
        if downloaded_files:
            downloaded_file = downloaded_files[0]
            shutil.move(str(downloaded_file), output_filename)
            print(f"[✓] Video downloaded successfully: {output_filename}")
            return True
        else:
            print("[!] No MP4 file found after download attempt")
            return False
            
    except Exception as e:
        print(f"[!] Error during Chrome download: {e}")
        return False
        
    finally:
        if driver:
            try:
                driver.quit()
                print("[+] Chrome driver closed")
            except:
                pass
        
        if temp_dir:
            try:
                shutil.rmtree(temp_dir)
                print("[+] Cleaned up temp directory")
            except:
                pass


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)