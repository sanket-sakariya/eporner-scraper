#!/usr/bin/env python3
"""
DLX Consumer Runner
Runs the DLX consumer to handle retry queues
"""

import logging
from consumer import DLXConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Main function to run DLX consumer"""
    try:
        logger.info("Starting DLX Consumer...")
        dlx_consumer = DLXConsumer()
        dlx_consumer.start_consuming_dlx()
    except KeyboardInterrupt:
        logger.info("DLX Consumer stopped by user")
    except Exception as e:
        logger.error(f"Error running DLX consumer: {e}")

if __name__ == "__main__":
    main()
