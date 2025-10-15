"""
Download an .mp4 file from eporner using Selenium with automatic download handling.
- Opens the page in Chrome (via webdriver-manager)
- Automatically handles download by configuring Chrome preferences
- No manual "Save As" dialog needed
"""

# pip install selenium webdriver-manager

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import shutil
from pathlib import Path
import zipfile
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


def kill_existing_chrome_processes():
    """Kill any existing Chrome processes to prevent conflicts."""
    import subprocess
    import signal
    
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


def create_proxy_auth_extension(proxy_host, proxy_port, proxy_username, proxy_password):
    """Create a Chrome extension for proxy authentication."""
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy Auth",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = f"""
    var config = {{
        mode: "fixed_servers",
        rules: {{
            singleProxy: {{
                scheme: "http",
                host: "{proxy_host}",
                port: parseInt({proxy_port})
            }},
            bypassList: ["localhost"]
        }}
    }};

    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

    function callbackFn(details) {{
        return {{
            authCredentials: {{
                username: "{proxy_username}",
                password: "{proxy_password}"
            }}
        }};
    }}

    chrome.webRequest.onAuthRequired.addListener(
        callbackFn,
        {{urls: ["<all_urls>"]}},
        ['blocking']
    );
    """

    # Create temporary directory for extension
    extension_dir = tempfile.mkdtemp(prefix="proxy_auth_extension_")
    
    # Write manifest.json
    with open(os.path.join(extension_dir, "manifest.json"), "w") as f:
        f.write(manifest_json)
    
    # Write background.js
    with open(os.path.join(extension_dir, "background.js"), "w") as f:
        f.write(background_js)
    
    return extension_dir


def check_chrome_installation():
    """Check if Chrome is properly installed and accessible."""
    import subprocess
    
    chrome_paths = [
        "/usr/bin/google-chrome",
        "/usr/bin/chromium-browser", 
        "/usr/bin/chromium",
        "/opt/google/chrome/chrome",
        "/usr/local/bin/chrome"
    ]
    
    chrome_found = False
    for path in chrome_paths:
        if shutil.which(path):
            chrome_found = True
            print(f"[+] Found Chrome at: {path}")
            break
    
    if not chrome_found:
        print("[!] Chrome not found in common locations")
        print("[!] Please install Chrome or Chromium:")
        print("    sudo apt update && sudo apt install -y google-chrome-stable")
        return False
    
    try:
        result = subprocess.run([shutil.which("google-chrome") or shutil.which("chromium-browser"), "--version"], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"[+] Chrome version: {result.stdout.strip()}")
    except Exception as e:
        print(f"[!] Error checking Chrome version: {e}")
    
    return True


def wait_for_download_complete(download_dir, timeout=300):
    """Wait for download to complete (no .crdownload files)."""
    print("[+] Waiting for download to complete...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        # Check for .crdownload files (Chrome's temp download files)
        crdownload_files = list(Path(download_dir).glob("*.crdownload"))
        tmp_files = list(Path(download_dir).glob("*.tmp"))
        
        if not crdownload_files and not tmp_files:
            # Check if any files exist in download directory
            files = [f for f in Path(download_dir).iterdir() if f.is_file()]
            if files:
                print(f"[✓] Download completed!")
                return True
        
        time.sleep(1)
        if int(time.time() - start_time) % 10 == 0:
            print(f"[+] Still downloading... ({int(time.time() - start_time)}s elapsed)")
    
    print("[!] Download timeout reached")
    return False


def main():
    # Check Chrome installation first
    print("[+] Checking Chrome installation...")
    if not check_chrome_installation():
        return False
    
    # Kill any existing Chrome processes
    print("[+] Cleaning up existing Chrome processes...")
    kill_existing_chrome_processes()
    
    # Setup download directory
    download_dir = os.path.join(os.getcwd(), "downloads")
    os.makedirs(download_dir, exist_ok=True)
    print(f"[+] Download directory: {download_dir}")
    
    # Initialize variables for cleanup
    extension_dir = None
    temp_dir = None
    
    # --- Setup Chrome with automatic download ---
    opts = Options()
    
    # Essential options for headless operation
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    
    # Create proxy authentication extension
    print(f"[+] Creating proxy authentication extension...")
    extension_dir = create_proxy_auth_extension(PROXY_IP, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD)
    opts.add_argument(f"--load-extension={extension_dir}")
    print(f"[+] Using proxy: {PROXY_IP}:{PROXY_PORT} with authentication")
    
    # Configure automatic downloads (NO POPUPS)
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,  # Disable "Save As" dialog
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,  # Disable safe browsing warnings
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "plugins.always_open_pdf_externally": True,  # Download PDFs instead of viewing
    }
    opts.add_experimental_option("prefs", prefs)
    
    # Create unique temp directory for user data
    import tempfile
    import uuid
    temp_dir = tempfile.mkdtemp(prefix=f"chrome_session_{uuid.uuid4().hex[:8]}_")
    opts.add_argument(f"--user-data-dir={temp_dir}")
    
    # Additional options
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--log-level=3")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = None
    try:
        # Initialize Chrome driver
        print(f"[+] Creating Chrome session...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
        print(f"[+] Chrome driver started successfully!")
        
        # Navigate to the video page first (optional, for context)
        print(f"[+] Navigating to video page: {VIDEO_PAGE_URL}")
        driver.get(VIDEO_PAGE_URL)
        time.sleep(3)
        print(f"[+] Page loaded successfully")
        
        # Now navigate directly to download URL
        print(f"[+] Starting download from: {DOWNLOAD_URL}")
        driver.get(DOWNLOAD_URL)
        
        # Wait for download to complete
        if wait_for_download_complete(download_dir, timeout=300):
            # Find the downloaded file
            downloaded_files = [f for f in Path(download_dir).iterdir() if f.is_file() and f.suffix == '.mp4']
            
            if downloaded_files:
                downloaded_file = downloaded_files[0]
                final_path = os.path.join(os.getcwd(), OUTPUT_FILENAME)
                
                # Move file to current directory with desired name
                shutil.move(str(downloaded_file), final_path)
                print(f"[✓] Video downloaded successfully: {final_path}")
                
                # Show file size
                file_size = os.path.getsize(final_path)
                print(f"[+] File size: {file_size / (1024*1024):.2f} MB")
                return True
            else:
                print("[!] No MP4 file found in download directory")
                return False
        else:
            print("[!] Download did not complete in time")
            return False
            
    except Exception as e:
        print(f"[!] Error during download: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        if driver:
            try:
                driver.quit()
                print("[+] Chrome driver closed successfully")
            except Exception as e:
                print(f"[!] Error closing Chrome driver: {e}")
        
        # Cleanup temp directory
        try:
            shutil.rmtree(temp_dir)
            print(f"[+] Cleaned up temp directory: {temp_dir}")
        except Exception as e:
            print(f"[!] Error cleaning up temp directory: {e}")
        
        # Cleanup extension directory
        if extension_dir:
            try:
                shutil.rmtree(extension_dir)
                print(f"[+] Cleaned up extension directory: {extension_dir}")
            except Exception as e:
                print(f"[!] Error cleaning up extension directory: {e}")
        
        # Cleanup download directory if empty
        try:
            if os.path.exists(download_dir) and not os.listdir(download_dir):
                shutil.rmtree(download_dir)
        except:
            pass


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)