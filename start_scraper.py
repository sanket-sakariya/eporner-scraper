#!/usr/bin/env python3
"""
Startup script for the eporner scraper system
"""
import subprocess
import sys
import time
import logging
import requests
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_rabbitmq():
    """Check if RabbitMQ is running"""
    try:
        import pika
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        connection.close()
        return True
    except Exception:
        return False

def check_postgres():
    """Check if PostgreSQL is running"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            dbname='e_data', #bp_data
            user='appuser', #postgres
            password='root',
            host='localhost',
            port='5432'
        )
        conn.close()
        return True
    except Exception:
        return False

def start_fastapi():
    """Start the FastAPI application"""
    logger.info("Starting FastAPI application...")
    try:
        subprocess.Popen([
            sys.executable, "-m", "uvicorn", 
            "main:app", 
            "--host", "0.0.0.0", 
            "--port", "8000", 
            "--reload"
        ])
        return True
    except Exception as e:
        logger.error(f"Failed to start FastAPI: {e}")
        return False

def start_consumers():
    """Start the consumer processes"""
    logger.info("Starting consumer processes...")
    try:
        # Start main consumers
        subprocess.Popen([sys.executable, "run_consumers.py"])
        logger.info("Started main consumers (run_consumers.py)")
        
        # Start DLX consumer
        subprocess.Popen([sys.executable, "run_dlx_consumer.py"])
        logger.info("Started DLX consumer (run_dlx_consumer.py)")
        
        return True
    except Exception as e:
        logger.error(f"Failed to start consumers: {e}")
        return False

def wait_for_api():
    """Wait for the API to be ready"""
    logger.info("Waiting for API to be ready...")
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            response = requests.get("http://localhost:8000/", timeout=5)
            if response.status_code == 200:
                logger.info("API is ready!")
                return True
        except Exception:
            pass
        
        time.sleep(2)
        logger.info(f"Waiting for API... (attempt {attempt + 1}/{max_attempts})")
    
    return False

def main():
    """Main startup function"""
    logger.info("Starting eporner scraper system...")
    
    # Check dependencies
    if not check_rabbitmq():
        logger.error("RabbitMQ is not running. Please start RabbitMQ first.")
        return False
    
    if not check_postgres():
        logger.error("PostgreSQL is not running or not accessible. Please check your database connection.")
        return False
    
    # Start services
    if not start_fastapi():
        logger.error("Failed to start FastAPI")
        return False
    
    if not start_consumers():
        logger.error("Failed to start consumers")
        return False
    
    # Wait for API to be ready
    if not wait_for_api():
        logger.error("API failed to start properly")
        return False
    
    logger.info("All services started successfully!")
    logger.info("FastAPI available at: http://localhost:8000")
    logger.info("API documentation at: http://localhost:8000/docs")
    
    # Keep the script running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
