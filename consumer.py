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
        
        # Declare main queues
        self.channel.queue_declare(queue='scraper_queue', durable=True)
        self.channel.queue_declare(queue='urls_queue', durable=True)
        self.channel.queue_declare(queue='data_queue', durable=True)
        
        # Declare DLX queues with progressive delays
        self.channel.queue_declare(queue='scraper_dlx_5min', durable=True, arguments={'x-message-ttl': 300000})  # 5 minutes
        self.channel.queue_declare(queue='scraper_dlx_10min', durable=True, arguments={'x-message-ttl': 600000})  # 10 minutes
        self.channel.queue_declare(queue='scraper_dlx_15min', durable=True, arguments={'x-message-ttl': 900000})  # 15 minutes
        self.channel.queue_declare(queue='scraper_dlx_failed', durable=True)  # Permanent failures
        
        # Declare exchanges
        self.channel.exchange_declare(exchange='scraper_dlx', exchange_type='direct', durable=True)
        
        # Bind DLX queues to exchange
        self.channel.queue_bind(exchange='scraper_dlx', queue='scraper_dlx_5min', routing_key='retry_5min')
        self.channel.queue_bind(exchange='scraper_dlx', queue='scraper_dlx_10min', routing_key='retry_10min')
        self.channel.queue_bind(exchange='scraper_dlx', queue='scraper_dlx_15min', routing_key='retry_15min')
        self.channel.queue_bind(exchange='scraper_dlx', queue='scraper_dlx_failed', routing_key='failed')
        
        self.channel.basic_qos(prefetch_count=1)

class ScraperConsumer(QueueConsumer):
    def __init__(self, rabbitmq_host='localhost'):
        super().__init__(rabbitmq_host)
        self.failed_urls = set()  # Track permanently failed URLs
    
    def process_message(self, ch, method, properties, body):
        """Process message from scraper_queue"""
        try:
            message = json.loads(body)
            url = message.get('url')
            retry_count = message.get('retry_count', 0)
            
            if not url:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Skip if already processed
            if self.db.is_url_processed(url):
                logger.info(f"URL already processed: {url}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Skip if permanently failed
            if url in self.failed_urls:
                logger.info(f"URL permanently failed, skipping: {url}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Scrape the URL
            scraped_data = self.scraper.scrape_url(url)
            
            # Check if scraping failed
            if not scraped_data.internal_links and not scraped_data.video_data:
                # Handle failed scraping with DLX
                self.handle_scraping_failure(url, retry_count, ch, method)
                return
            
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
    
    def handle_scraping_failure(self, url, retry_count, ch, method):
        """Handle scraping failure with progressive DLX retry"""
        retry_count += 1
        
        if retry_count <= 1:
            # First failure: retry after 5 minutes
            logger.warning(f"🔄 First failure for {url}, retrying in 5 minutes (attempt {retry_count})")
            self.publish_to_dlx_queue(url, retry_count, 'retry_5min')
        elif retry_count <= 2:
            # Second failure: retry after 10 minutes
            logger.warning(f"🔄 Second failure for {url}, retrying in 10 minutes (attempt {retry_count})")
            self.publish_to_dlx_queue(url, retry_count, 'retry_10min')
        elif retry_count <= 3:
            # Third failure: retry after 15 minutes
            logger.warning(f"🔄 Third failure for {url}, retrying in 15 minutes (attempt {retry_count})")
            self.publish_to_dlx_queue(url, retry_count, 'retry_15min')
        else:
            # Permanent failure: mark as failed
            logger.error(f"❌ Permanent failure for {url} after {retry_count} attempts")
            self.failed_urls.add(url)
            self.publish_to_dlx_queue(url, retry_count, 'failed')
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def publish_to_dlx_queue(self, url, retry_count, routing_key):
        """Publish URL to DLX queue for retry"""
        message = json.dumps({
            'url': url,
            'retry_count': retry_count,
            'timestamp': time.time()
        })
        
        self.channel.basic_publish(
            exchange='scraper_dlx',
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        logger.info(f"Published {url} to DLX queue: {routing_key}")
    
    
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

class DLXConsumer(QueueConsumer):
    """Consumer for DLX retry queues"""
    def __init__(self, rabbitmq_host='localhost'):
        super().__init__(rabbitmq_host)
        self.failed_urls = set()  # Track permanently failed URLs
    
    def process_dlx_message(self, ch, method, properties, body):
        """Process message from DLX queue"""
        try:
            message = json.loads(body)
            url = message.get('url')
            retry_count = message.get('retry_count', 0)
            
            if not url:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Skip if permanently failed
            if url in self.failed_urls:
                logger.info(f"URL permanently failed, skipping: {url}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Skip if already processed
            if self.db.is_url_processed(url):
                logger.info(f"URL already processed: {url}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            logger.info(f"🔄 Retrying URL from DLX: {url} (attempt {retry_count})")
            
            # Try scraping again
            scraped_data = self.scraper.scrape_url(url)
            
            # Check if scraping succeeded
            if scraped_data.internal_links or scraped_data.video_data:
                # Success: mark as processed and handle data
                self.db.mark_url_processed(url)
                
                # Send internal links to urls_queue
                for internal_link in scraped_data.internal_links:
                    if not self.db.is_url_processed(internal_link):
                        self.publish_to_urls_queue(internal_link)
                
                # Send video data to data_queue
                if scraped_data.video_data:
                    self.publish_to_data_queue(scraped_data.video_data)
                
                logger.info(f"✅ DLX retry successful: {url}")
            else:
                # Still failed: handle next retry level
                self.handle_dlx_retry_failure(url, retry_count, ch, method)
                return
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logger.error(f"Error processing DLX message: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def handle_dlx_retry_failure(self, url, retry_count, ch, method):
        """Handle DLX retry failure"""
        if retry_count <= 1:
            # Move to 10-minute retry
            logger.warning(f"🔄 DLX retry failed for {url}, moving to 10-minute retry")
            self.publish_to_dlx_queue(url, retry_count + 1, 'retry_10min')
        elif retry_count <= 2:
            # Move to 15-minute retry
            logger.warning(f"🔄 DLX retry failed for {url}, moving to 15-minute retry")
            self.publish_to_dlx_queue(url, retry_count + 1, 'retry_15min')
        else:
            # Permanent failure
            logger.error(f"❌ DLX permanent failure for {url} after {retry_count} attempts")
            self.failed_urls.add(url)
            self.publish_to_dlx_queue(url, retry_count + 1, 'failed')
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def publish_to_dlx_queue(self, url, retry_count, routing_key):
        """Publish URL to DLX queue for retry"""
        message = json.dumps({
            'url': url,
            'retry_count': retry_count,
            'timestamp': time.time()
        })
        
        self.channel.basic_publish(
            exchange='scraper_dlx',
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        logger.info(f"Published {url} to DLX queue: {routing_key}")
    
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
    
    def start_consuming_dlx(self):
        """Start consuming from DLX queues"""
        logger.info("Starting DLX Consumer...")
        
        # Consume from all DLX retry queues
        self.channel.basic_consume(
            queue='scraper_dlx_5min',
            on_message_callback=self.process_dlx_message
        )
        self.channel.basic_consume(
            queue='scraper_dlx_10min',
            on_message_callback=self.process_dlx_message
        )
        self.channel.basic_consume(
            queue='scraper_dlx_15min',
            on_message_callback=self.process_dlx_message
        )
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("DLX Consumer stopped by user")
            self.channel.stop_consuming()