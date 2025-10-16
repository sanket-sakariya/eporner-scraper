#!/usr/bin/env python3
"""
Minimal Webshare.io Proxy Fetcher - 100 lines
Fetches 100 proxies and stores them in database
"""

import requests
import logging
from database import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_KEY = "urrnlju19pn4sywl2oq6l9e4ecxme1s8j65ljfq8"
BASE_URL = "https://proxy.webshare.io/api/v2"
PROXY_COUNT = 100

def fetch_proxies():
    """Fetch proxies from Webshare.io API"""
    try:
        logger.info("Fetching 100 proxies...")
        
        headers = {'Authorization': f'Token {API_KEY}'}
        params = {'page': 1, 'page_size': PROXY_COUNT, 'mode': 'direct'}
        
        response = requests.get(f"{BASE_URL}/proxy/list/", headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            proxies = data.get('results', [])
            logger.info(f"Fetched {len(proxies)} proxies")
            return proxies
        else:
            logger.error(f"API Error: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        return []

def save_proxies(proxies):
    """Save proxies to database"""
    try:
        db = DatabaseManager()
        saved = 0
        
        for proxy in proxies:
            proxy_id = str(proxy.get('id', ''))
            ip = proxy.get('proxy_address', '').strip()
            port = proxy.get('port')
            username = proxy.get('username', '').strip()
            password = proxy.get('password', '').strip()
            country = proxy.get('country_code', 'Unknown')
            city = proxy.get('city', 'Unknown')
            
            if all([proxy_id, ip, port, username, password]):
                try:
                    port = int(port)
                    success = db.save_proxy(proxy_id, ip, port, username, password, country, city)
                    if success:
                        saved += 1
                        logger.info(f"Saved: {ip}:{port} ({country})")
                except:
                    continue
        
        logger.info(f"Saved {saved}/{len(proxies)} proxies to database")
        return saved > 0
        
    except Exception as e:
        logger.error(f"Save error: {e}")
        return False

def main():
    """Main function"""
    logger.info("Starting proxy fetcher...")
    
    # Clear existing proxies
    db = DatabaseManager()
    db.clear_all_proxies()
    logger.info("Cleared existing proxies")
    
    # Fetch and save proxies
    proxies = fetch_proxies()
    if proxies:
        success = save_proxies(proxies)
        if success:
            logger.info("✅ Proxy fetching completed successfully")
        else:
            logger.error("❌ Failed to save proxies")
    else:
        logger.error("❌ No proxies fetched")

if __name__ == "__main__":
    main()