# Eporner Scraper

An asynchronous web scraper for eporner.com using FastAPI, RabbitMQ, and PostgreSQL.

## Architecture

The scraper follows a queue-based architecture with three main components:

1. **FastAPI Application** (`main.py`) - REST API for managing the scraping process
2. **Scraper** (`scraper.py`) - Asynchronous web scraper using aiohttp
3. **Consumers** (`consumer.py`) - Queue processors for handling URLs and data
4. **Database** (`database.py`) - PostgreSQL database operations

## Workflow

1. **Initial Request**: Send URLs to the scraper via FastAPI
2. **URL Processing**: URLs are added to `urls_queue`
3. **Scraping**: `scraper_consumer` processes URLs from `scraper_queue`
4. **Data Extraction**: Extracts internal links, video data, MP4/JPG URLs, view counts, and like counts
5. **Queue Distribution**: 
   - Internal URLs → `urls_queue` → `urls_consumer` → Database + back to `scraper_queue`
   - Video data → `data_queue` → `data_consumer` → Database

## Prerequisites

- Python 3.8+
- RabbitMQ server
- PostgreSQL database
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Setup PostgreSQL database:
   ```sql
   CREATE DATABASE bp_data;
   ```

4. Update database configuration in `database.py` if needed

## Usage

### Quick Start

Run the complete system:
```bash
python start_scraper.py
```

This will:
- Check RabbitMQ and PostgreSQL connections
- Start the FastAPI server on port 8000
- Start all consumer processes
- Wait for the API to be ready

### Manual Start

1. Start RabbitMQ server
2. Start PostgreSQL database
3. Start the FastAPI application:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```
4. Start the consumers:
   ```bash
   python run_consumers.py
   ```

### API Usage

#### Start Scraping
```bash
curl -X POST "http://localhost:8000/start-scraping" \
     -H "Content-Type: application/json" \
     -d '{"urls": ["https://www.eporner.com/"]}'
```

#### Get Statistics
```bash
curl "http://localhost:8000/stats"
```

#### Get URLs
```bash
curl "http://localhost:8000/urls?limit=100"
```

#### Get Videos
```bash
curl "http://localhost:8000/videos?limit=100"
```

### API Documentation

Visit `http://localhost:8000/docs` for interactive API documentation.

## Configuration

### Database Configuration
Update `database.py` with your PostgreSQL credentials:
```python
self.db_config = {
    'dbname': 'bp_data',
    'user': 'postgres',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}
```

### RabbitMQ Configuration
Default configuration uses localhost. Update consumer classes if needed:
```python
consumer = ScraperConsumer(rabbitmq_host='your_rabbitmq_host')
```

## Features

- **Reliable Requests**: Uses requests library with proper headers and session management
- **Queue-based Architecture**: RabbitMQ for reliable message processing
- **Batch Processing**: Memory-based deduplication and batch database operations for high performance
- **Retry Mechanism**: Automatic retry with exponential backoff
- **Data Deduplication**: Prevents processing duplicate URLs
- **Comprehensive Parsing**: Extracts video data, links, and metadata
- **REST API**: Easy integration and monitoring
- **Error Handling**: Robust error handling and logging

## Data Structure

### URLs Table
- `id`: Primary key
- `url`: URL string
- `domain`: Extracted domain
- `is_processed`: Processing status
- `created_at`: Creation timestamp
- `processed_at`: Processing timestamp

### Video Data Table
- `id`: Primary key
- `video_url`: Video page URL
- `view_count`: Number of views
- `like_count`: Number of likes
- `mp4_links`: JSON array of MP4 URLs
- `jpg_links`: JSON array of JPG URLs
- `created_at`: Creation timestamp
- `updated_at`: Update timestamp

## Monitoring

- Check API status: `http://localhost:8000/`
- View statistics: `http://localhost:8000/stats`
- Monitor logs for consumer activity
- Use RabbitMQ management interface for queue monitoring

## Troubleshooting

### Common Issues

1. **RabbitMQ Connection Error**
   - Ensure RabbitMQ server is running
   - Check connection parameters

2. **PostgreSQL Connection Error**
   - Verify database credentials
   - Ensure PostgreSQL is running
   - Check database exists

3. **Consumer Not Processing**
   - Check RabbitMQ queues
   - Verify consumer processes are running
   - Check logs for errors

4. **Scraping Failures**
   - Check network connectivity
   - Verify target website accessibility
   - Review error logs

### Logs

All components use Python's logging module. Check console output for detailed information about:
- URL processing status
- Database operations
- Queue operations
- Error messages

## Development

### Adding New Parsers

1. Extend `EpornerScraper` class in `scraper.py`
2. Add new extraction methods
3. Update `extract_video_data` method
4. Test with sample URLs

### Customizing Selectors

Update CSS selectors in `scraper.py` methods:
- `extract_view_count()`
- `extract_like_count()`
- `extract_mp4_links()`
- `extract_jpg_links()`

### Performance Optimization

The system uses several optimizations for maximum performance:

#### Batch Processing
- **URLs Consumer**: Processes URLs in batches of 100 with 3-second timeout
- **Data Consumer**: Processes video data in batches of 50 with 10-second timeout
- **Memory Deduplication**: Uses in-memory sets to avoid duplicate processing
- **Batch Database Operations**: Uses `executemany()` for efficient database inserts

#### Smart URL Processing
- **Video URLs** (contain `/video`): Extract video data + internal links
- **Non-video URLs**: Extract only internal links (much faster)
- **URL Type Detection**: Automatically detects video URLs for optimized processing

#### Performance Benefits
- **Faster crawling**: Non-video pages processed much faster
- **Reduced processing**: Only video pages extract heavy data
- **Better throughput**: Scraper consumer can handle more URLs per second

Test the optimized scraper:
```bash
python test_scraper.py
```

## License

This project is for educational purposes only. Please respect the terms of service of the target website.
