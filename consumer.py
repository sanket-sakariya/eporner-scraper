import pika
import json
import logging
import time
from scraper import EpornerScraper
from database import DatabaseManager
from models import URLMessage, ScrapedData, VideoData
import asyncio

logger = logging.getLogger(__name__)

class QueueConsumer:
    def __init__(self, rabbitmq_host='localhost'):
        self.rabbitmq_host = rabbitmq_host
        self.scraper = EpornerScraper()
        self.db = DatabaseManager()
        
        # Setup RabbitMQ connection
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitmq_host)
        )
        self.channel = self.connection.channel()
        
        # Declare queues
        self.channel.queue_declare(queue='scraper_queue', durable=True)
        self.channel.queue_declare(queue='urls_queue', durable=True)
        self.channel.queue_declare(queue='data_queue', durable=True)
        
        self.channel.basic_qos(prefetch_count=1)

class ScraperConsumer(QueueConsumer):
    def __init__(self, rabbitmq_host='localhost'):
        super().__init__(rabbitmq_host)
    
    def process_message(self, ch, method, properties, body):
        """Process message from scraper_queue"""
        try:
            message = json.loads(body)
            url = message.get('url')
            
            if not url:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Skip if already processed
            if self.db.is_url_processed(url):
                logger.info(f"URL already processed: {url}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Scrape the URL
            scraped_data = self.scraper.scrape_url(url)
            
            # Mark URL as processed
            self.db.mark_url_processed(url)
            
            # Send internal links to urls_queue
            for internal_link in scraped_data.internal_links:
                if not self.db.is_url_processed(internal_link):
                    self.publish_to_urls_queue(internal_link)
            
            # Send video data to data_queue
            if scraped_data.video_data:
                self.publish_to_data_queue(scraped_data.video_data)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Successfully processed: {url}")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    
    def publish_to_urls_queue(self, url):
        """Publish URL to urls_queue"""
        message = json.dumps({'url': url})
        self.channel.basic_publish(
            exchange='',
            routing_key='urls_queue',
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
    
    def publish_to_data_queue(self, video_data):
        """Publish video data to data_queue"""
        message = json.dumps(video_data.dict())
        self.channel.basic_publish(
            exchange='',
            routing_key='data_queue',
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
    
    def start_consuming(self):
        """Start consuming from scraper_queue"""
        logger.info("Starting Scraper Consumer...")
        self.channel.basic_consume(
            queue='scraper_queue',
            on_message_callback=self.process_message
        )
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Scraper Consumer stopped by user")
            self.channel.stop_consuming()

class URLsConsumer(QueueConsumer):
    def __init__(self, rabbitmq_host='localhost'):
        super().__init__(rabbitmq_host)
        self.url_buffer = set()  # In-memory deduplication
        self.batch_size = 100  # Process URLs in batches
        self.last_batch_time = time.time()
        self.batch_timeout = 3  # Process batch every 5 seconds
    
    def process_message(self, ch, method, properties, body):
        """Process message from urls_queue"""
        try:
            message = json.loads(body)
            url = message.get('url')
            
            if url:
                # Add to memory buffer for deduplication
                if url not in self.url_buffer:
                    self.url_buffer.add(url)
                    logger.debug(f"Added URL to buffer: {url}")
                
                # Check if we should process the batch
                if (len(self.url_buffer) >= self.batch_size or 
                    time.time() - self.last_batch_time >= self.batch_timeout):
                    self.process_batch()
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logger.error(f"Error processing URL message: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def process_batch(self):
        """Process a batch of URLs"""
        if not self.url_buffer:
            return
        
        try:
            logger.info(f"Processing batch of {len(self.url_buffer)} URLs")
            
            # Get existing URLs from database to avoid duplicates
            existing_urls = self.db.get_existing_urls(list(self.url_buffer))
            
            # Filter out existing URLs
            new_urls = [url for url in self.url_buffer if url not in existing_urls]
            
            if new_urls:
                # Batch insert new URLs
                if self.db.batch_save_urls(new_urls):
                    logger.info(f"Batch saved {len(new_urls)} new URLs")
                    
                    # Send all new URLs to scraper_queue
                    for url in new_urls:
                        self.publish_to_scraper_queue(url)
                else:
                    logger.error("Failed to batch save URLs")
            else:
                logger.info("No new URLs to process")
            
            # Clear buffer and update timestamp
            self.url_buffer.clear()
            self.last_batch_time = time.time()
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Clear buffer even on error to prevent memory issues
            self.url_buffer.clear()
    
    def publish_to_scraper_queue(self, url):
        """Publish URL to scraper_queue"""
        message = json.dumps({'url': url})
        self.channel.basic_publish(
            exchange='',
            routing_key='scraper_queue',
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
    
    def start_consuming(self):
        """Start consuming from urls_queue"""
        logger.info("Starting URLs Consumer...")
        self.channel.basic_consume(
            queue='urls_queue',
            on_message_callback=self.process_message
        )
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("URLs Consumer stopped by user")
            # Process any remaining URLs in buffer before stopping
            if self.url_buffer:
                logger.info(f"Processing final batch of {len(self.url_buffer)} URLs")
                self.process_batch()
            self.channel.stop_consuming()

class DataConsumer(QueueConsumer):
    def __init__(self, rabbitmq_host='localhost'):
        super().__init__(rabbitmq_host)
        self.data_buffer = []  # Buffer for video data
        self.batch_size = 50  # Process video data in batches
        self.last_batch_time = time.time()
        self.batch_timeout = 10  # Process batch every 10 seconds
    
    def process_message(self, ch, method, properties, body):
        """Process message from data_queue"""
        try:
            message = json.loads(body)
            
            # Convert to VideoData object
            video_data_dict = json.loads(message) if isinstance(message, str) else message
            video_data = VideoData(**video_data_dict)
            
            # Add to buffer
            self.data_buffer.append(video_data)
            logger.debug(f"Added video data to buffer: {video_data.video_url}")
            
            # Check if we should process the batch
            if (len(self.data_buffer) >= self.batch_size or 
                time.time() - self.last_batch_time >= self.batch_timeout):
                self.process_batch()
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logger.error(f"Error processing data message: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def process_batch(self):
        """Process a batch of video data"""
        if not self.data_buffer:
            return
        
        try:
            logger.info(f"Processing batch of {len(self.data_buffer)} video data items")
            
            # Batch save video data
            if self.db.batch_save_video_data(self.data_buffer):
                logger.info(f"Batch saved {len(self.data_buffer)} video data items")
            else:
                logger.error("Failed to batch save video data")
            
            # Clear buffer and update timestamp
            self.data_buffer.clear()
            self.last_batch_time = time.time()
            
        except Exception as e:
            logger.error(f"Error processing video data batch: {e}")
            # Clear buffer even on error to prevent memory issues
            self.data_buffer.clear()
    
    def start_consuming(self):
        """Start consuming from data_queue"""
        logger.info("Starting Data Consumer...")
        self.channel.basic_consume(
            queue='data_queue',
            on_message_callback=self.process_message
        )
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Data Consumer stopped by user")
            # Process any remaining video data in buffer before stopping
            if self.data_buffer:
                logger.info(f"Processing final batch of {len(self.data_buffer)} video data items")
                self.process_batch()
            self.channel.stop_consuming()