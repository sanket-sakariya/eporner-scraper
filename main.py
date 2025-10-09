from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List
import pika
import json
import logging
from database import DatabaseManager
from models import URLMessage
import uvicorn

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Eporner Scraper API", version="1.0.0")

# Initialize components
db = DatabaseManager()

class StartScrapingRequest(BaseModel):
    urls: List[str]

@app.on_event("startup")
async def startup_event():
    """Initialize RabbitMQ connection on startup"""
    logger.info("Eporner Scraper API started")

@app.get("/")
async def root():
    return {"message": "Eporner Scraper API", "status": "running"}

@app.post("/start-scraping")
async def start_scraping(request: StartScrapingRequest, background_tasks: BackgroundTasks):
    """Start scraping process with initial URLs"""
    try:
        # Setup RabbitMQ connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        
        # Declare queues
        channel.queue_declare(queue='scraper_queue', durable=True)
        channel.queue_declare(queue='urls_queue', durable=True)
        channel.queue_declare(queue='data_queue', durable=True)
        
        # Add initial URLs to urls_queue
        for url in request.urls:
            message = json.dumps({'url': url})
            channel.basic_publish(
                exchange='',
                routing_key='urls_queue',
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(f"Queued initial URL: {url}")
        
        connection.close()
        
        return {
            "status": "success",
            "message": f"Started scraping with {len(request.urls)} initial URLs",
            "queues_initialized": ["scraper_queue", "urls_queue", "data_queue"]
        }
    
    except Exception as e:
        logger.error(f"Error starting scraping: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Get scraping statistics"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get URL statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_urls,
                COUNT(CASE WHEN is_processed THEN 1 END) as processed_urls,
                COUNT(CASE WHEN NOT is_processed THEN 1 END) as pending_urls
            FROM urls
        """)
        url_stats = cursor.fetchone()
        
        # Get video data statistics
        cursor.execute("SELECT COUNT(*) as total_videos FROM video_data")
        video_stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "urls": {
                "total": url_stats[0],
                "processed": url_stats[1],
                "pending": url_stats[2]
            },
            "videos": {
                "total": video_stats[0]
            }
        }
    
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/urls")
async def get_urls(limit: int = 100, processed: bool = None):
    """Get URLs from database"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        if processed is None:
            cursor.execute("SELECT url, is_processed, created_at FROM urls ORDER BY created_at DESC LIMIT %s", (limit,))
        else:
            cursor.execute("SELECT url, is_processed, created_at FROM urls WHERE is_processed = %s ORDER BY created_at DESC LIMIT %s", (processed, limit))
        
        urls = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "urls": [
                {
                    "url": row[0],
                    "is_processed": row[1],
                    "created_at": row[2].isoformat() if row[2] else None
                }
                for row in urls
            ]
        }
    
    except Exception as e:
        logger.error(f"Error getting URLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/videos")
async def get_videos(limit: int = 100):
    """Get video data from database"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT video_url, view_count, like_count, mp4_links, jpg_links, updated_at 
            FROM video_data 
            ORDER BY updated_at DESC 
            LIMIT %s
        """, (limit,))
        
        videos = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "videos": [
                {
                    "video_url": row[0],
                    "view_count": row[1],
                    "like_count": row[2],
                    "mp4_links": row[3],
                    "jpg_links": row[4],
                    "updated_at": row[5].isoformat() if row[5] else None
                }
                for row in videos
            ]
        }
    
    except Exception as e:
        logger.error(f"Error getting videos: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)