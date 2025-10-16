#!/usr/bin/env python3
"""
Webshare.io Proxy Fetcher
Fetches proxy list from Webshare.io API and stores them in database
"""

import requests
import json
import time
import logging
from database import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Webshare.io API Configuration
WEBSHARE_API_KEY = "urrnlju19pn4sywl2oq6l9e4ecxme1s8j65ljfq8"  # Replace with your actual API key
WEBSHARE_BASE_URL = "https://proxy.webshare.io/api/v2"
PROXY_COUNT = 100  # Number of proxies to fetch

class WebshareProxyFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.db = DatabaseManager()
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Token {api_key}',
            'Content-Type': 'application/json'
        })
    
    def fetch_proxy_list(self):
        """Fetch proxy list from Webshare.io API"""
        try:
            logger.info(f"🔍 Fetching {PROXY_COUNT} proxies from Webshare.io...")
            
            all_proxies = []
            page = 1
            page_size = min(100, PROXY_COUNT)  # API limit per page
            
            while len(all_proxies) < PROXY_COUNT:
                # Webshare.io API endpoint for proxy list
                url = f"{WEBSHARE_BASE_URL}/proxy/list/"
                
                # Parameters for proxy list
                params = {
                    'page': page,
                    'page_size': page_size,
                    'format': 'json'
                }
                
                logger.info(f"📄 Fetching page {page}...")
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    proxies = data.get('results', [])
                    
                    if not proxies:
                        logger.info("📄 No more proxies available")
                        break
                    
                    all_proxies.extend(proxies)
                    logger.info(f"📄 Page {page}: Got {len(proxies)} proxies (Total: {len(all_proxies)})")
                    
                    # Check if we have enough proxies
                    if len(all_proxies) >= PROXY_COUNT:
                        all_proxies = all_proxies[:PROXY_COUNT]
                        break
                    
                    page += 1
                    
                    # Small delay to avoid rate limiting
                    time.sleep(0.5)
                    
                elif response.status_code == 401:
                    logger.error("❌ Authentication failed - check your API key")
                    return []
                elif response.status_code == 403:
                    logger.error("❌ Access forbidden - check your API permissions")
                    return []
                elif response.status_code == 429:
                    logger.warning("⚠️  Rate limited - waiting 10 seconds...")
                    time.sleep(10)
                    continue
                else:
                    logger.error(f"❌ Failed to fetch proxies: {response.status_code} - {response.text}")
                    return []
                
        except Exception as e:
            logger.error(f"❌ Error fetching proxy list: {e}")
            return []
        
        logger.info(f"✅ Successfully fetched {len(all_proxies)} proxies")
        return all_proxies
    
    def fetch_proxy_credentials(self, proxy_id):
        """Fetch proxy credentials for a specific proxy"""
        try:
            url = f"{WEBSHARE_BASE_URL}/proxy/{proxy_id}/"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"⚠️  Failed to fetch credentials for proxy {proxy_id}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.warning(f"⚠️  Error fetching credentials for proxy {proxy_id}: {e}")
            return None
    
    def save_proxies_to_db(self, proxies):
        """Save proxies to database"""
        try:
            logger.info(f"💾 Saving {len(proxies)} proxies to database...")
            
            saved_count = 0
            skipped_count = 0
            error_count = 0
            
            for i, proxy in enumerate(proxies, 1):
                try:
                    # Extract proxy details
                    proxy_id = str(proxy.get('id', ''))
                    ip = proxy.get('proxy_address', '').strip()
                    port = proxy.get('port')
                    username = proxy.get('username', '').strip()
                    password = proxy.get('password', '').strip()
                    country = proxy.get('country_code', 'Unknown')
                    city = proxy.get('city', 'Unknown')
                    is_active = proxy.get('is_active', True)
                    
                    # Validate required fields
                    if not all([proxy_id, ip, port, username, password]):
                        logger.warning(f"⚠️  [{i}/{len(proxies)}] Incomplete proxy data for ID {proxy_id}, skipping")
                        skipped_count += 1
                        continue
                    
                    # Convert port to integer
                    try:
                        port = int(port)
                    except (ValueError, TypeError):
                        logger.warning(f"⚠️  [{i}/{len(proxies)}] Invalid port {port} for proxy {proxy_id}, skipping")
                        skipped_count += 1
                        continue
                    
                    # Save to database
                    success = self.db.save_proxy(
                        proxy_id=proxy_id,
                        ip=ip,
                        port=port,
                        username=username,
                        password=password,
                        country=country,
                        city=city,
                        is_active=is_active
                    )
                    
                    if success:
                        saved_count += 1
                        logger.info(f"✅ [{i}/{len(proxies)}] Saved proxy: {ip}:{port} ({country})")
                    else:
                        logger.warning(f"⚠️  [{i}/{len(proxies)}] Failed to save proxy: {ip}:{port}")
                        error_count += 1
                    
                    # Small delay to avoid overwhelming the database
                    if i % 10 == 0:  # Every 10 proxies
                        time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"❌ [{i}/{len(proxies)}] Error saving proxy {proxy.get('id', 'unknown')}: {e}")
                    error_count += 1
                    continue
            
            logger.info(f"📊 Save Summary:")
            logger.info(f"   ✅ Successfully saved: {saved_count}")
            logger.info(f"   ⚠️  Skipped (incomplete data): {skipped_count}")
            logger.info(f"   ❌ Errors: {error_count}")
            logger.info(f"   📈 Total processed: {saved_count + skipped_count + error_count}/{len(proxies)}")
            
            return saved_count
            
        except Exception as e:
            logger.error(f"❌ Error saving proxies to database: {e}")
            return 0
    
    def test_proxy_connection(self, ip, port, username, password):
        """Test if proxy is working"""
        try:
            proxy_url = f"http://{username}:{password}@{ip}:{port}"
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            
            test_url = "http://httpbin.org/ip"
            response = requests.get(test_url, proxies=proxies, timeout=10)
            
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, None
                
        except Exception as e:
            return False, str(e)
    
    def run(self):
        """Main execution function"""
        try:
            logger.info("🚀 Starting Webshare.io proxy fetcher...")
            
            # Check if we already have proxies in database
            existing_proxies = self.db.get_all_proxies(active_only=False)
            if existing_proxies:
                logger.info(f"📊 Found {len(existing_proxies)} existing proxies in database")
                
                # Ask user if they want to clear existing proxies
                response = input("Do you want to clear existing proxies and fetch new ones? (y/N): ").strip().lower()
                if response == 'y':
                    logger.info("🗑️  Clearing existing proxies...")
                    self.db.clear_all_proxies()
                else:
                    logger.info("ℹ️  Keeping existing proxies, adding new ones...")
            
            # Fetch proxy list
            proxies = self.fetch_proxy_list()
            
            if not proxies:
                logger.error("❌ No proxies fetched, exiting")
                return False
            
            # Save proxies to database
            saved_count = self.save_proxies_to_db(proxies)
            
            if saved_count > 0:
                logger.info(f"🎉 Successfully processed {saved_count} proxies")
                
                # Get final statistics
                stats = self.db.get_proxy_stats()
                if stats:
                    logger.info(f"📊 Final Database Statistics:")
                    logger.info(f"   📈 Total proxies: {stats.get('total', 0)}")
                    logger.info(f"   ✅ Active proxies: {stats.get('active', 0)}")
                    logger.info(f"   ❌ Inactive proxies: {stats.get('inactive', 0)}")
                    logger.info(f"   ⚠️  Failed proxies: {stats.get('failed', 0)}")
                    logger.info(f"   📊 Average failures: {stats.get('avg_failures', 0):.2f}")
                
                # Test a few proxies
                logger.info("🔍 Testing proxy connections...")
                test_count = min(5, saved_count)
                working_count = 0
                
                # Get random proxies for testing
                test_proxies = self.db.get_all_proxies(active_only=True)[:test_count]
                
                for i, proxy in enumerate(test_proxies):
                    ip = proxy['ip']
                    port = proxy['port']
                    username = proxy['username']
                    password = proxy['password']
                    
                    is_working, result = self.test_proxy_connection(ip, port, username, password)
                    if is_working:
                        working_count += 1
                        logger.info(f"✅ Test {i+1}: {ip}:{port} - Working")
                        # Mark as successful
                        self.db.mark_proxy_success(proxy['proxy_id'])
                    else:
                        logger.warning(f"⚠️  Test {i+1}: {ip}:{port} - Not working")
                        # Mark as failed
                        self.db.mark_proxy_failure(proxy['proxy_id'])
                
                logger.info(f"📊 Test results: {working_count}/{test_count} proxies working")
                return True
            else:
                logger.error("❌ No proxies saved to database")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in main execution: {e}")
            return False

def main():
    """Main function"""
    print("=" * 60)
    print("🌐 Webshare.io Proxy Fetcher")
    print("=" * 60)
    
    # Get API key from user
    api_key = input("Enter your Webshare.io API key: ").strip()
    
    if not api_key:
        logger.error("❌ No API key provided, exiting")
        return
    
    if api_key == "your_webshare_api_key_here":
        logger.error("❌ Please enter a valid Webshare.io API key")
        return
    
    print(f"\n🔑 Using API key: {api_key[:8]}...")
    print(f"🎯 Target: {PROXY_COUNT} proxies")
    print("-" * 60)
    
    try:
        fetcher = WebshareProxyFetcher(api_key)
        success = fetcher.run()
        
        print("\n" + "=" * 60)
        if success:
            logger.info("✅ Proxy fetching completed successfully")
            print("🎉 All done! Your proxies are ready to use.")
        else:
            logger.error("❌ Proxy fetching failed")
            print("💥 Something went wrong. Check the logs above.")
        print("=" * 60)
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Operation cancelled by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main()