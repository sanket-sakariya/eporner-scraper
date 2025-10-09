#!/usr/bin/env python3
"""
Script to run all consumers for the eporner scraper
"""
import asyncio
import logging
import signal
import sys
from consumer import ScraperConsumer, URLsConsumer, DataConsumer
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConsumerManager:
    def __init__(self):
        self.consumers = []
        self.running = True
        
    def start_consumers(self):
        """Start all consumers in separate threads"""
        try:
            # Start Scraper Consumer
            scraper_consumer = ScraperConsumer()
            scraper_thread = threading.Thread(
                target=scraper_consumer.start_consuming,
                name="ScraperConsumer"
            )
            scraper_thread.daemon = True
            scraper_thread.start()
            self.consumers.append(scraper_thread)
            logger.info("Started Scraper Consumer")
            
            # Start URLs Consumer
            urls_consumer = URLsConsumer()
            urls_thread = threading.Thread(
                target=urls_consumer.start_consuming,
                name="URLsConsumer"
            )
            urls_thread.daemon = True
            urls_thread.start()
            self.consumers.append(urls_thread)
            logger.info("Started URLs Consumer")
            
            # Start Data Consumer
            data_consumer = DataConsumer()
            data_thread = threading.Thread(
                target=data_consumer.start_consuming,
                name="DataConsumer"
            )
            data_thread.daemon = True
            data_thread.start()
            self.consumers.append(data_thread)
            logger.info("Started Data Consumer")
            
        except Exception as e:
            logger.error(f"Error starting consumers: {e}")
            self.running = False
    
    def stop_consumers(self):
        """Stop all consumers gracefully"""
        logger.info("Stopping all consumers...")
        self.running = False
        
        # Wait for all threads to finish
        for thread in self.consumers:
            if thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("All consumers stopped")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down...")
    consumer_manager.stop_consumers()
    sys.exit(0)

if __name__ == "__main__":
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer_manager = ConsumerManager()
    
    try:
        consumer_manager.start_consumers()
        
        # Keep the main thread alive
        while consumer_manager.running:
            try:
                # Check if all consumer threads are still alive
                alive_threads = [t for t in consumer_manager.consumers if t.is_alive()]
                if not alive_threads:
                    logger.error("All consumer threads have died, exiting...")
                    break
                
                # Sleep for a bit
                threading.Event().wait(1)
                
            except KeyboardInterrupt:
                break
                
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        consumer_manager.stop_consumers()
