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
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_urls_url ON urls(url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_urls_processed ON urls(is_processed)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_video_data_url ON video_data(video_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_diskwala_url ON diskwala_data(diskwala_url)")
            
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
        """Get video data that hasn't been uploaded to DiskWala yet"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT vd.id, vd.video_url, vd.view_count, vd.like_count, vd.mp4_links, vd.jpg_links
            FROM video_data vd
            LEFT JOIN diskwala_data dd ON vd.video_url = dd.video_url
            WHERE dd.video_url IS NULL
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