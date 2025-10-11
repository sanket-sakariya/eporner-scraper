# file: extract_links_requests.py
import requests
from bs4 import BeautifulSoup

URL = "https://www.eporner.com/video-IL7JTGmJ6Jo/friends-enjoying-with-indian-girlfriend-in-a-hotel-room/"
OUTPUT_FILE = "extracted_links.txt"

headers = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://google.com/",
    "Connection": "keep-alive",
}

def extract_links_requests(url: str):
    s = requests.Session()
    s.headers.update(headers)
    resp = s.get(url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    links = [a.get("href") for a in soup.select("a[href]")]
    # Normalize relative URLs to absolute
    from urllib.parse import urljoin
    links = [urljoin(resp.url, href) for href in links if href]
    # dedupe but preserve order
    unique_links = list(dict.fromkeys(links))
    return unique_links

if __name__ == "__main__":
    print("Fetching HTML and extracting links (requests)...")
    links = extract_links_requests(URL)
    print(f"Found {len(links)} unique links")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for link in links:
            f.write(link + "\n")
    print(f"Links saved to '{OUTPUT_FILE}'")
