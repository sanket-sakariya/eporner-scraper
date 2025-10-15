"""
Download an .mp4 file from eporner using Selenium + requests + webdriver-manager.
- Opens the referer page in Chrome (via webdriver-manager)
- Injects cookies (from your curl)
- Extracts valid cookies
- Streams the actual video file with requests (safe for large files)
"""

# pip install selenium webdriver-manager requests

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import requests
import time
from requests.cookies import cookiejar_from_dict
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import ssl


# ---------------- CONFIG ----------------
REFERER_PAGE = "https://www.eporner.com/video-Ux4DafGxvAz/hidden-lust/"
DOWNLOAD_URL = "https://www.eporner.com/dload/Ux4DafGxvAz/240/14177153-240p-av1.mp4?click=1"
OUTFILE = "14177153-240p-av1.mp4"
COOKIE_STRING = (
    "EPRNS=1f1f2fc16fde6abea678fcec8358f5ff; "
    "PHPSESSID=f693e8026ee5bd3a7ac9a913ac307b65; "
    "ADBpcount=1; ADBp=yes; epcolor=black; ap.Ux4DafGxvAz=true"
)
CHUNK_SIZE = 1024 * 1024  # 1 MB

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    "Referer": REFERER_PAGE,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Upgrade-Insecure-Requests": "1",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}
# ----------------------------------------


def parse_cookie_string(cookie_str):
    """Convert 'k=v; k2=v2' string to dict."""
    d = {}
    for part in cookie_str.split(";"):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            k, v = part.split("=", 1)
            d[k.strip()] = v.strip()
    return d


def selenium_get_cookies(driver, domain=None):
    """Extract Selenium cookies to dict."""
    cookies = {}
    for c in driver.get_cookies():
        if domain and domain not in c.get("domain", ""):
            continue
        cookies[c["name"]] = c["value"]
    return cookies


def create_robust_session(cookies_dict):
    """Create a requests session with retry logic and proper SSL handling."""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    # Mount adapter with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set headers and cookies
    session.headers.update(HEADERS)
    session.cookies = cookiejar_from_dict(cookies_dict)
    
    # Configure SSL context to be more permissive
    session.verify = True  # Keep SSL verification but handle errors gracefully
    
    return session


def download_with_retry(session, url, output_file, max_retries=3):
    """Download file with retry logic for connection errors."""
    for attempt in range(max_retries):
        try:
            print(f"[+] Starting download (attempt {attempt + 1}/{max_retries})...")
            
            with session.get(url, stream=True, timeout=120, allow_redirects=True) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                bytes_written = 0
                
                with open(output_file, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue
                        f.write(chunk)
                        bytes_written += len(chunk)
                        if total:
                            pct = bytes_written / total * 100
                            print(f"\rDownloaded {bytes_written}/{total} bytes ({pct:.2f}%)", end="", flush=True)
                
                print(f"\n[✓] Download finished: {output_file}")
                return True
                
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout,
                ConnectionResetError,
                ssl.SSLError) as e:
            print(f"\n[!] Connection error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"[+] Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"[!] All {max_retries} attempts failed")
                return False
        except Exception as e:
            print(f"\n[!] Unexpected error: {e}")
            return False
    
    return False


def main():
    # --- Setup Chrome via webdriver-manager ---
    opts = Options()
    # opts.add_argument("--headless=new")  # optional, comment out if you need GUI
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    try:
        # Visit referer page
        driver.get(REFERER_PAGE)
        time.sleep(2)

        # Inject cookies (from curl)
        parsed = urlparse(REFERER_PAGE)
        cookie_domain = parsed.hostname
        cookies_from_string = parse_cookie_string(COOKIE_STRING)
        for name, value in cookies_from_string.items():
            cookie_dict = {"name": name, "value": value, "domain": cookie_domain, "path": "/"}
            try:
                driver.add_cookie(cookie_dict)
            except Exception as e:
                print(f"[warn] Could not add cookie {name}: {e}")

        # Reload page so cookies apply
        driver.get(REFERER_PAGE)
        time.sleep(1)

        # Extract cookies from live browser
        selenium_cookies = selenium_get_cookies(driver, domain=cookie_domain)
        merged_cookies = cookies_from_string.copy()
        merged_cookies.update(selenium_cookies)

    finally:
        driver.quit()

    # --- Download file via requests with retry logic ---
    print(f"[+] Creating robust session with {len(merged_cookies)} cookies...")
    sess = create_robust_session(merged_cookies)
    
    # Attempt download with retry logic
    success = download_with_retry(sess, DOWNLOAD_URL, OUTFILE)
    
    if not success:
        print("[!] Download failed after all retry attempts")
        return False
    
    return True


if __name__ == "__main__":
    main()
