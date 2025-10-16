import psycopg2
from psycopg2.extras import Json
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_config=None):
        self.db_config = db_config or {
            'dbname': 'e_data',
            'user': 'appuser',
            'password': 'root',
            'host': 'localhost',
            'port': '5432'
        }
        
        # Initialize database tables
        self.init_tables()
    
    def get_connection(self):
        return psycopg2.connect(**self.db_config)
    
    def init_tables(self):
        """Initialize database tables"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # URLs table for storing discovered URLs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    domain VARCHAR(255),
                    is_processed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP
                )
            """)
            
            # Video data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS video_data (
                    id SERIAL PRIMARY KEY,
                    video_url TEXT UNIQUE NOT NULL,
                    view_count TEXT,
                    like_count TEXT,
                    mp4_links JSONB,
                    jpg_links JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # DiskWala data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS diskwala_data (
                    id SERIAL PRIMARY KEY,
                    diskwala_url TEXT UNIQUE NOT NULL,
                    jpg_image_link TEXT,
                    mp4_link TEXT,
                    video_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Processed videos table - tracks all video URLs that have been processed
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_videos (
                    id SERIAL PRIMARY KEY,
                    video_url TEXT UNIQUE NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    reason TEXT,
                    file_size_mb DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Proxies table - stores Webshare.io proxy information
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS proxies (
                    id SERIAL PRIMARY KEY,
                    proxy_id VARCHAR(50) UNIQUE NOT NULL,
                    ip VARCHAR(45) NOT NULL,
                    port INTEGER NOT NULL,
                    username VARCHAR(255) NOT NULL,
                    password VARCHAR(255) NOT NULL,
                    country VARCHAR(10),
                    city VARCHAR(255),
                    is_active BOOLEAN DEFAULT TRUE,
                    last_used TIMESTAMP,
                    failure_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_urls_url ON urls(url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_urls_processed ON urls(is_processed)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_video_data_url ON video_data(video_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_diskwala_url ON diskwala_data(diskwala_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_processed_videos_url ON processed_videos(video_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_processed_videos_status ON processed_videos(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_proxies_proxy_id ON proxies(proxy_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_proxies_ip_port ON proxies(ip, port)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_proxies_active ON proxies(is_active)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_proxies_country ON proxies(country)")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("Database tables initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database tables: {e}")
    
    def save_url(self, url):
        """Save URL to database if not exists"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO urls (url, domain)
            VALUES (%s, %s)
            ON CONFLICT (url) DO NOTHING
            """
            
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
            
            cursor.execute(insert_query, (url, domain))
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving URL: {e}")
            return False
    
    def mark_url_processed(self, url):
        """Mark URL as processed"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            update_query = """
            UPDATE urls 
            SET is_processed = TRUE, processed_at = CURRENT_TIMESTAMP
            WHERE url = %s
            """
            
            cursor.execute(update_query, (url,))
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error marking URL as processed: {e}")
            return False
    
    def save_video_data(self, video_data):
        """Save video data to database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO video_data (video_url, view_count, like_count, mp4_links, jpg_links)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (video_url) 
            DO UPDATE SET 
                view_count = EXCLUDED.view_count,
                like_count = EXCLUDED.like_count,
                mp4_links = EXCLUDED.mp4_links,
                jpg_links = EXCLUDED.jpg_links,
                updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_query, (
                video_data.video_url,
                video_data.view_count,
                video_data.like_count,
                Json(video_data.mp4_links),
                Json(video_data.jpg_links)
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Video data saved for: {video_data.video_url}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving video data: {e}")
            return False
    
    def is_url_processed(self, url):
        """Check if URL is already processed"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT id FROM urls WHERE url = %s AND is_processed = TRUE", (url,))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error checking URL: {e}")
            return False
    
    def get_existing_urls(self, urls):
        """Get existing URLs from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['%s'] * len(urls))
            query = f"SELECT url FROM urls WHERE url IN ({placeholders})"
            
            cursor.execute(query, urls)
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            # Return set of existing URLs
            return set(row[0] for row in results)
            
        except Exception as e:
            logger.error(f"Error getting existing URLs: {e}")
            return set()
    
    def batch_save_urls(self, urls):
        """Batch save URLs to database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Prepare data for batch insert
            url_data = []
            for url in urls:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc
                url_data.append((url, domain))
            
            # Batch insert with ON CONFLICT DO NOTHING
            insert_query = """
            INSERT INTO urls (url, domain)
            VALUES (%s, %s)
            ON CONFLICT (url) DO NOTHING
            """
            
            cursor.executemany(insert_query, url_data)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            logger.info(f"Batch inserted {len(urls)} URLs")
            return True
            
        except Exception as e:
            logger.error(f"Error batch saving URLs: {e}")
            return False
    
    def batch_save_video_data(self, video_data_list):
        """Batch save video data to database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Prepare data for batch insert
            video_data_tuples = []
            for video_data in video_data_list:
                video_data_tuples.append((
                    video_data.video_url,
                    video_data.view_count,
                    video_data.like_count,
                    Json(video_data.mp4_links),
                    Json(video_data.jpg_links)
                ))
            
            # Batch insert with ON CONFLICT DO UPDATE
            insert_query = """
            INSERT INTO video_data (video_url, view_count, like_count, mp4_links, jpg_links)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (video_url) 
            DO UPDATE SET 
                view_count = EXCLUDED.view_count,
                like_count = EXCLUDED.like_count,
                mp4_links = EXCLUDED.mp4_links,
                jpg_links = EXCLUDED.jpg_links,
                updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.executemany(insert_query, video_data_tuples)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            logger.info(f"Batch inserted {len(video_data_list)} video data items")
            return True
            
        except Exception as e:
            logger.error(f"Error batch saving video data: {e}")
            return False
    
    def get_video_data_for_download(self, limit=10):
        """Get video data that hasn't been uploaded to DiskWala yet and hasn't been processed"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT vd.id, vd.video_url, vd.view_count, vd.like_count, vd.mp4_links, vd.jpg_links
            FROM video_data vd
            LEFT JOIN diskwala_data dd ON vd.video_url = dd.video_url
            LEFT JOIN processed_videos pv ON vd.video_url = pv.video_url
            WHERE dd.video_url IS NULL
            AND pv.video_url IS NULL
            AND vd.mp4_links IS NOT NULL 
            AND jsonb_array_length(vd.mp4_links) > 0
            AND vd.jpg_links IS NOT NULL 
            AND jsonb_array_length(vd.jpg_links) > 0
            ORDER BY vd.created_at ASC
            LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            # Convert results to list of dictionaries
            video_data_list = []
            for row in results:
                video_data_list.append({
                    'id': row[0],
                    'video_url': row[1],
                    'view_count': row[2],
                    'like_count': row[3],
                    'mp4_links': row[4],
                    'jpg_links': row[5]
                })
            
            return video_data_list
            
        except Exception as e:
            logger.error(f"Error getting video data for download: {e}")
            return []
    
    def is_video_already_uploaded(self, video_url):
        """Check if video URL already exists in diskwala_data table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT id FROM diskwala_data WHERE video_url = %s", (video_url,))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error checking if video already uploaded: {e}")
            return False
    
    def save_diskwala_data(self, diskwala_url, jpg_image_link, mp4_link, video_url):
        """Save DiskWala data to database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO diskwala_data (diskwala_url, jpg_image_link, mp4_link, video_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (diskwala_url) 
            DO UPDATE SET 
                jpg_image_link = EXCLUDED.jpg_image_link,
                mp4_link = EXCLUDED.mp4_link,
                video_url = EXCLUDED.video_url,
                updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_query, (diskwala_url, jpg_image_link, mp4_link, video_url))
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"DiskWala data saved: {diskwala_url}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving DiskWala data: {e}")
            return False
    
    def is_video_processed(self, video_url):
        """Check if video URL has already been processed"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT id, status, reason FROM processed_videos WHERE video_url = %s", (video_url,))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result is not None, result[1] if result else None, result[2] if result else None
            
        except Exception as e:
            logger.error(f"Error checking if video processed: {e}")
            return False, None, None
    
    def mark_video_processed(self, video_url, status, reason=None, file_size_mb=None):
        """Mark video as processed with status and reason"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO processed_videos (video_url, status, reason, file_size_mb)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (video_url) 
            DO UPDATE SET 
                status = EXCLUDED.status,
                reason = EXCLUDED.reason,
                file_size_mb = EXCLUDED.file_size_mb,
                updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_query, (video_url, status, reason, file_size_mb))
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Video marked as processed: {video_url} - Status: {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error marking video as processed: {e}")
            return False
    
    def get_processed_videos_stats(self):
        """Get statistics of processed videos"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    AVG(file_size_mb) as avg_file_size_mb
                FROM processed_videos 
                GROUP BY status
                ORDER BY count DESC
            """)
            
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            stats = {}
            for row in results:
                stats[row[0]] = {
                    'count': row[1],
                    'avg_file_size_mb': float(row[2]) if row[2] else 0
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting processed videos stats: {e}")
            return {}
    
    def save_proxy(self, proxy_id, ip, port, username, password, country=None, city=None, is_active=True):
        """Save a single proxy to database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO proxies (proxy_id, ip, port, username, password, country, city, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (proxy_id) 
            DO UPDATE SET 
                ip = EXCLUDED.ip,
                port = EXCLUDED.port,
                username = EXCLUDED.username,
                password = EXCLUDED.password,
                country = EXCLUDED.country,
                city = EXCLUDED.city,
                is_active = EXCLUDED.is_active,
                updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_query, (proxy_id, ip, port, username, password, country, city, is_active))
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving proxy: {e}")
            return False
    
    def get_all_proxies(self, active_only=True):
        """Get all proxies from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if active_only:
                query = "SELECT * FROM proxies WHERE is_active = TRUE ORDER BY failure_count ASC, created_at ASC"
            else:
                query = "SELECT * FROM proxies ORDER BY failure_count ASC, created_at ASC"
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            proxies = []
            for row in results:
                proxies.append({
                    'id': row[0],
                    'proxy_id': row[1],
                    'ip': row[2],
                    'port': row[3],
                    'username': row[4],
                    'password': row[5],
                    'country': row[6],
                    'city': row[7],
                    'is_active': row[8],
                    'last_used': row[9],
                    'failure_count': row[10],
                    'created_at': row[11],
                    'updated_at': row[12]
                })
            
            return proxies
            
        except Exception as e:
            logger.error(f"Error getting proxies: {e}")
            return []
    
    def get_random_proxy(self):
        """Get a random active proxy"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT * FROM proxies 
            WHERE is_active = TRUE 
            ORDER BY RANDOM() 
            LIMIT 1
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result:
                return {
                    'id': result[0],
                    'proxy_id': result[1],
                    'ip': result[2],
                    'port': result[3],
                    'username': result[4],
                    'password': result[5],
                    'country': result[6],
                    'city': result[7],
                    'is_active': result[8],
                    'last_used': result[9],
                    'failure_count': result[10],
                    'created_at': result[11],
                    'updated_at': result[12]
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting random proxy: {e}")
            return None
    
    def mark_proxy_failure(self, proxy_id):
        """Mark proxy as failed and increment failure count"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            update_query = """
            UPDATE proxies 
            SET failure_count = failure_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE proxy_id = %s
            """
            
            cursor.execute(update_query, (proxy_id,))
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error marking proxy failure: {e}")
            return False
    
    def mark_proxy_success(self, proxy_id):
        """Mark proxy as successful and reset failure count"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            update_query = """
            UPDATE proxies 
            SET failure_count = 0,
                last_used = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE proxy_id = %s
            """
            
            cursor.execute(update_query, (proxy_id,))
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error marking proxy success: {e}")
            return False
    
    def deactivate_proxy(self, proxy_id):
        """Deactivate a proxy"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            update_query = """
            UPDATE proxies 
            SET is_active = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE proxy_id = %s
            """
            
            cursor.execute(update_query, (proxy_id,))
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error deactivating proxy: {e}")
            return False
    
    def get_proxy_stats(self):
        """Get proxy statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active,
                    COUNT(CASE WHEN is_active = FALSE THEN 1 END) as inactive,
                    COUNT(CASE WHEN failure_count > 0 THEN 1 END) as failed,
                    AVG(failure_count) as avg_failures
                FROM proxies
            """)
            
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result:
                return {
                    'total': result[0],
                    'active': result[1],
                    'inactive': result[2],
                    'failed': result[3],
                    'avg_failures': float(result[4]) if result[4] else 0
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting proxy stats: {e}")
            return {}
    
    def clear_all_proxies(self):
        """Clear all proxies from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM proxies")
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("All proxies cleared from database")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing proxies: {e}")
            return False