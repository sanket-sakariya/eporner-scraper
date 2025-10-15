"""
Download an .mp4 file from eporner using requests with residential proxy.
- Cross-platform support (Windows, Linux, macOS)
- Uses requests for direct HTTP downloads (fastest method)
- Supports residential proxy with authentication
- Fallback to selenium with proxy if needed
- Compatible with Python 3.13+
- Automatic Chrome installation detection and process management
"""

# pip install requests selenium webdriver-manager

import requests
import time
import os
import shutil
import platform
import subprocess
import signal
from pathlib import Path
import tempfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


# ---------------- CONFIG ----------------
VIDEO_PAGE_URL = "https://www.eporner.com/video-Ux4DafGxvAz/hidden-lust/"
DOWNLOAD_URL = "https://www.eporner.com/dload/Ux4DafGxvAz/240/14177153-240p-av1.mp4?click=1"
OUTPUT_FILENAME = "14177153-240p-av1.mp4"

# Proxy configuration from dev.txt
PROXY_IP = "191.96.254.130"
PROXY_PORT = "6177"
PROXY_USERNAME = "tjijutki"
PROXY_PASSWORD = "4vg93ifc50gnx"

# Configuration options
SKIP_PROXY_TEST = False  # Set to True to skip proxy testing and proceed directly
# ----------------------------------------


def kill_existing_chrome_processes():
    """Kill any existing Chrome processes to prevent conflicts (cross-platform)."""
    try:
        system = platform.system().lower()
        
        if system == "windows":
            # Windows: Use taskkill command
            try:
                result = subprocess.run(['taskkill', '/f', '/im', 'chrome.exe'], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    print("[+] Killed existing Chrome processes on Windows")
                else:
                    print("[+] No existing Chrome processes found on Windows")
            except Exception as e:
                print(f"[!] Error killing Chrome processes on Windows: {e}")
                
        elif system in ["linux", "darwin"]:  # Linux or macOS
            # Unix-like systems: Use pgrep and kill
            try:
                result = subprocess.run(['pgrep', '-f', 'chrome'], capture_output=True, text=True)
                if result.returncode == 0 and result.stdout.strip():
                    pids = result.stdout.strip().split('\n')
                    print(f"[+] Found {len(pids)} existing Chrome processes, killing them...")
                    for pid in pids:
                        try:
                            os.kill(int(pid), signal.SIGTERM)
                            print(f"[+] Killed Chrome process {pid}")
                        except (ProcessLookupError, ValueError):
                            pass
                    time.sleep(2)
                else:
                    print("[+] No existing Chrome processes found")
            except Exception as e:
                print(f"[!] Error killing Chrome processes: {e}")
        else:
            print(f"[!] Unsupported platform: {system}")
            
    except Exception as e:
        print(f"[!] Error in kill_existing_chrome_processes: {e}")


def check_chrome_installation():
    """Check if Chrome is properly installed and accessible (cross-platform)."""
    try:
        system = platform.system().lower()
        chrome_paths = []
        
        if system == "windows":
            chrome_paths = [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                r"C:\Users\{}\AppData\Local\Google\Chrome\Application\chrome.exe".format(os.getenv('USERNAME', '')),
            ]
        elif system == "linux":
            chrome_paths = [
                "/usr/bin/google-chrome",
                "/usr/bin/chromium-browser", 
                "/usr/bin/chromium",
                "/opt/google/chrome/chrome",
                "/usr/local/bin/chrome"
            ]
        elif system == "darwin":  # macOS
            chrome_paths = [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            ]
        
        chrome_found = False
        chrome_path = None
        
        for path in chrome_paths:
            if os.path.exists(path):
                chrome_found = True
                chrome_path = path
                print(f"[+] Found Chrome at: {path}")
                break
        
        if not chrome_found:
            print(f"[!] Chrome not found on {system}")
            if system == "windows":
                print("[!] Please install Chrome from: https://www.google.com/chrome/")
            elif system == "linux":
                print("[!] Please install Chrome:")
                print("    sudo apt update && sudo apt install -y google-chrome-stable")
            elif system == "darwin":
                print("[!] Please install Chrome from: https://www.google.com/chrome/")
            return False
        
        # Test Chrome version
        try:
            if system == "windows":
                result = subprocess.run([chrome_path, "--version"], 
                                      capture_output=True, text=True, timeout=10)
            else:
                result = subprocess.run([chrome_path, "--version"], 
                                      capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                print(f"[+] Chrome version: {result.stdout.strip()}")
            else:
                print(f"[!] Could not get Chrome version")
        except Exception as e:
            print(f"[!] Error checking Chrome version: {e}")
        
        return True
        
    except Exception as e:
        print(f"[!] Error in check_chrome_installation: {e}")
        return False


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
        
        response = requests.get(download_url, proxies=proxies, headers=headers, stream=True, timeout=60)
        
        # Handle different response codes
        if response.status_code == 503:
            print(f"[!] Server overloaded (503) - retrying in 5 seconds...")
            time.sleep(5)
            response = requests.get(download_url, proxies=proxies, headers=headers, stream=True, timeout=60)
        
        if response.status_code not in [200, 206]:
            print(f"[!] Download failed with status: {response.status_code}")
            if response.status_code == 403:
                print("[!] Access forbidden - the download link may have expired or require different authentication")
            elif response.status_code == 404:
                print("[!] File not found - the download link may be invalid")
            elif response.status_code == 429:
                print("[!] Rate limited - too many requests")
            return False
        
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
    """Test if proxy connection is working with multiple fallback methods."""
    try:
        print(f"[+] Testing proxy connection: {proxy_config['ip']}:{proxy_config['port']}")
        
        proxies = {
            'http': f'http://{proxy_config["username"]}:{proxy_config["password"]}@{proxy_config["ip"]}:{proxy_config["port"]}',
            'https': f'http://{proxy_config["username"]}:{proxy_config["password"]}@{proxy_config["ip"]}:{proxy_config["port"]}'
        }
        
        # Test multiple endpoints
        test_urls = [
            'http://httpbin.org/ip',
            'https://httpbin.org/ip',
            'http://ipinfo.io/ip',
            'https://api.ipify.org',
            'http://icanhazip.com'
        ]
        
        for url in test_urls:
            try:
                print(f"[+] Testing with {url}...")
                response = requests.get(url, proxies=proxies, timeout=15)
                
                if response.status_code == 200:
                    print(f"[✓] Proxy connection successful via {url}")
                    return True
                elif response.status_code == 503:
                    print(f"[!] Proxy server overloaded (503) - trying next endpoint...")
                    continue
                elif response.status_code == 407:
                    print(f"[!] Proxy authentication failed (407) - check credentials")
                    return False
                else:
                    print(f"[!] Proxy test failed with status: {response.status_code} - trying next endpoint...")
                    continue
                    
            except requests.exceptions.ProxyError as e:
                print(f"[!] Proxy error with {url}: {e}")
                continue
            except requests.exceptions.Timeout:
                print(f"[!] Timeout with {url} - trying next endpoint...")
                continue
            except Exception as e:
                print(f"[!] Error testing {url}: {e}")
                continue
        
        print(f"[!] All proxy test endpoints failed")
        return False
            
    except Exception as e:
        print(f"[!] Proxy connection test failed: {e}")
        return False


def test_proxy_with_download_url(proxy_config, download_url):
    """Test proxy specifically with the download URL."""
    try:
        print(f"[+] Testing proxy with download URL...")
        
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
        
        # Test with HEAD request first (faster)
        response = requests.head(download_url, proxies=proxies, headers=headers, timeout=15)
        
        if response.status_code in [200, 206, 302, 301]:
            print(f"[✓] Proxy works with download URL (status: {response.status_code})")
            return True
        elif response.status_code == 503:
            print(f"[!] Download server overloaded (503) - proxy may still work")
            return True  # Allow to proceed as proxy might work for actual download
        else:
            print(f"[!] Download URL test failed with status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"[!] Download URL proxy test failed: {e}")
        return False


def main():
    system = platform.system()
    print(f"[+] Starting eporner video downloader with residential proxy on {system}...")
    
    # Check Chrome installation first
    print("[+] Checking Chrome installation...")
    if not check_chrome_installation():
        print("[!] Chrome installation check failed. Please install Chrome.")
        return False
    
    # Kill any existing Chrome processes
    print("[+] Cleaning up existing Chrome processes...")
    kill_existing_chrome_processes()
    
    # Setup proxy configuration
    proxy_config = {
        "ip": PROXY_IP,
        "port": PROXY_PORT,
        "username": PROXY_USERNAME,
        "password": PROXY_PASSWORD
    }
    
    print(f"[+] Using residential proxy: {PROXY_IP}:{PROXY_PORT}")
    
    # Test proxy connection (unless skipped)
    if SKIP_PROXY_TEST:
        print("[+] Skipping proxy test (SKIP_PROXY_TEST=True)")
        proxy_test_passed = True
    else:
        proxy_test_passed = test_proxy_connection(proxy_config)
        
        if not proxy_test_passed:
            print("[!] General proxy test failed, trying with download URL...")
            proxy_test_passed = test_proxy_with_download_url(proxy_config, DOWNLOAD_URL)
        
        if not proxy_test_passed:
            print("[!] All proxy tests failed.")
            print("[?] Do you want to proceed anyway? The proxy might still work for the actual download.")
            print("[+] Proceeding with download attempt...")
            print("[!] Note: If download fails, please check your proxy settings.")
        else:
            print("[✓] Proxy connection verified!")
    
    # Setup output file path (cross-platform)
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
        print("[!] Direct download failed, trying with Chrome/selenium...")
        return download_with_chrome(DOWNLOAD_URL, output_path, proxy_config)


def download_with_chrome(download_url, output_filename, proxy_config):
    """Fallback method using selenium with proxy (cross-platform)."""
    driver = None
    temp_dir = None
    
    try:
        system = platform.system().lower()
        print(f"[+] Setting up Chrome with selenium on {system}...")
        
        # Create temp directory for Chrome user data
        temp_dir = tempfile.mkdtemp(prefix="chrome_session_")
        
        # Setup Chrome options
        options = Options()
        options.add_argument(f"--user-data-dir={temp_dir}")
        
        # Platform-specific Chrome options
        if system == "linux":
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--headless=new")
        elif system == "windows":
            options.add_argument("--disable-gpu")
            options.add_argument("--headless=new")
            options.add_argument("--disable-web-security")
        elif system == "darwin":  # macOS
            options.add_argument("--disable-gpu")
            options.add_argument("--headless=new")
        
        # Common options
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")
        
        # Setup proxy with authentication
        proxy_server = f"{proxy_config['ip']}:{proxy_config['port']}"
        options.add_argument(f"--proxy-server=http://{proxy_server}")
        
        # Setup download preferences (cross-platform)
        download_dir = os.path.dirname(output_filename)
        # Normalize path for Windows
        if system == "windows":
            download_dir = download_dir.replace('/', '\\')
        
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        options.add_experimental_option("prefs", prefs)
        
        # Initialize driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        print(f"[+] Chrome driver started with proxy: {proxy_server}")
        
        # Navigate to download URL
        print(f"[+] Navigating to download URL...")
        driver.get(download_url)
        
        # Wait for download to start and complete
        print(f"[+] Waiting for download to complete...")
        time.sleep(10)  # Give it time to start
        
        # Check if download completed
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
        import traceback
        traceback.print_exc()
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