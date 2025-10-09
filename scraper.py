import requests
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging
from models import ScrapedData, VideoData
import re
import time

logger = logging.getLogger(__name__)

class EpornerScraper:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://google.com/",
            "Connection": "keep-alive",
        }
        self.session.headers.update(self.headers)
    
    def scrape_url(self, url, max_retries=3):
        """Scrape a single URL and extract data with retry mechanism"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Scraping URL: {url} (attempt {attempt + 1})")
                
                response = self.session.get(url, timeout=20)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check if this is a video URL for faster processing
                is_video_url = '/video' in url.lower()
                
                # Extract video data only for video URLs
                video_data = None
                if is_video_url:
                    video_data = self.extract_video_data(url, soup)
                
                # Extract all internal links (always needed for crawling)
                internal_links = self.extract_internal_links(url, soup)
                
                logger.info(f"Processed {'video' if is_video_url else 'page'} URL: {url}")
                
                return ScrapedData(
                    url=url,
                    video_data=video_data,
                    internal_links=internal_links
                )
                
            except requests.RequestException as e:
                logger.warning(f"Request error scraping {url} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to scrape {url} after {max_retries} attempts")
                    return ScrapedData(url=url, internal_links=[])
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except Exception as e:
                logger.error(f"Error scraping {url} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return ScrapedData(url=url, internal_links=[])
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def extract_video_data(self, url, soup):
        """Extract video data from page"""
        try:
            # Only extract video data if URL contains '/video'
            if '/video' not in url.lower():
                logger.debug(f"Skipping video data extraction for non-video URL: {url}")
                return None
            
            logger.info(f"Extracting video data from video URL: {url}")
            
            view_count = self.extract_view_count(soup)
            like_count = self.extract_like_count(soup)
            mp4_links = self.extract_mp4_links(url, soup)
            jpg_links = self.extract_jpg_links(url, soup)
            
            return VideoData(
                video_url=url,
                view_count=view_count or 'N/A',
                like_count=like_count or 'N/A',
                mp4_links=mp4_links,
                jpg_links=jpg_links
            )
            
        except Exception as e:
            logger.error(f"Error extracting video data from {url}: {e}")
            return None
    
    def is_video_page(self, soup):
        """Check if the page is a video page"""
        # eporner.com specific video page indicators
        video_indicators = [
            soup.find('div', class_=re.compile(r'video-player|player|video-container')),
            soup.find('div', id=re.compile(r'video-player|player|video-container')),
            soup.find('meta', property='og:type', content='video'),
            soup.find('link', rel='canonical', href=re.compile(r'/video/')),
            soup.find('video'),
            soup.find('source', src=re.compile(r'\.mp4')),
            soup.find('div', class_=re.compile(r'porn-video')),
            soup.find('div', id=re.compile(r'porn-video')),
            soup.find('script', string=re.compile(r'video|player', re.I))
        ]
        return any(video_indicators)
    
    def extract_view_count(self, soup):
        """Extract view count from page"""
        view_selectors = [
            '*[id*="views"]',
            '*[id*="cinemaviews"]',
            '.views',
            '*[title*="Views"]',
            '*[class*="views"]',
            '*[class*="view-count"]',
            '.view-count',
            '.video-views',
            '*[data-views]',
            '*[data-view-count]',
            # eporner.com specific selectors
            '.stats .views',
            '.video-stats .views',
            '.info .views',
            '*[class*="stat"][class*="view"]'
        ]
        
        for selector in view_selectors:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                text = element.get_text(strip=True)
                # Extract numbers from text
                numbers = re.findall(r'[\d,]+', text)
                if numbers:
                    return numbers[0]
        
        return None
    
    def extract_like_count(self, soup):
        """Extract like count from page"""
        like_selectors = [
            '*[id*="like"]',
            '*[class*="like"]',
            '*[title*="like"]',
            '.rating',
            '*[class*="rating"]',
            '*[class*="vote"]',
            '.like-count',
            '.video-likes',
            '*[data-likes]',
            '*[data-like-count]',
            # eporner.com specific selectors
            '.stats .likes',
            '.video-stats .likes',
            '.info .likes',
            '*[class*="stat"][class*="like"]',
            '.vote-up',
            '.thumbs-up'
        ]
        
        for selector in like_selectors:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                text = element.get_text(strip=True)
                # Extract numbers from text
                numbers = re.findall(r'[\d,]+', text)
                if numbers:
                    return numbers[0]
        
        return None
    
    def extract_mp4_links(self, base_url, soup):
        """Extract all MP4 links from page"""
        mp4_links = set()
        
        # Find in video sources
        video_sources = soup.find_all('source', src=re.compile(r'\.mp4', re.I))
        for source in video_sources:
            src = source.get('src')
            if src:
                full_url = urljoin(base_url, src)
                mp4_links.add(full_url)
        
        # Find in links
        links = soup.find_all('a', href=re.compile(r'\.mp4', re.I))
        for link in links:
            href = link.get('href')
            if href:
                full_url = urljoin(base_url, href)
                mp4_links.add(full_url)
        
        # Find in video elements
        videos = soup.find_all('video')
        for video in videos:
            src = video.get('src')
            if src and src.endswith('.mp4'):
                full_url = urljoin(base_url, src)
                mp4_links.add(full_url)
        
        # Find in JavaScript/JSON data
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string:
                # Look for MP4 URLs in script content
                mp4_matches = re.findall(r'https?://[^\s"\']+\.mp4', script.string, re.I)
                for match in mp4_matches:
                    mp4_links.add(match)
        
        return list(mp4_links)
    
    def extract_jpg_links(self, base_url, soup):
        """Extract all JPG links from page"""
        jpg_links = set()
        
        # Find in images
        images = soup.find_all('img', src=re.compile(r'\.jpg|\.jpeg', re.I))
        for img in images:
            src = img.get('src')
            if src:
                full_url = urljoin(base_url, src)
                jpg_links.add(full_url)
        
        # Find in links
        links = soup.find_all('a', href=re.compile(r'\.jpg|\.jpeg', re.I))
        for link in links:
            href = link.get('href')
            if href:
                full_url = urljoin(base_url, href)
                jpg_links.add(full_url)
        
        # Find in data attributes
        elements_with_data = soup.find_all(attrs={'data-src': re.compile(r'\.jpg|\.jpeg', re.I)})
        for element in elements_with_data:
            src = element.get('data-src')
            if src:
                full_url = urljoin(base_url, src)
                jpg_links.add(full_url)
        
        # Find in JavaScript/JSON data
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string:
                # Look for JPG URLs in script content
                jpg_matches = re.findall(r'https?://[^\s"\']+\.jpg', script.string, re.I)
                for match in jpg_matches:
                    jpg_links.add(match)
        
        return list(jpg_links)
    
    def extract_internal_links(self, base_url, soup):
        """Extract all internal links from page"""
        internal_links = set()
        base_domain = urlparse(base_url).netloc
        
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href')
            if href and not href.startswith(('javascript:', 'mailto:', 'tel:', '#', 'data:')):
                full_url = urljoin(base_url, href)
                parsed_url = urlparse(full_url)
                
                # Check if it's internal link and from eporner.com
                if parsed_url.netloc == base_domain or 'eporner.com' in parsed_url.netloc:
                    # Filter out unwanted URLs
                    if not any(exclude in full_url.lower() for exclude in [
                        '/logout', '/login', '/register', '/contact', '/about',
                        '/terms', '/privacy', '/dmca', '/sitemap', '/rss',
                        '.css', '.js', '.png', '.gif', '.ico', '.xml'
                    ]):
                        internal_links.add(full_url)
        
        logger.info(f"Extracted {len(internal_links)} internal links from {base_url}")
        return list(internal_links)