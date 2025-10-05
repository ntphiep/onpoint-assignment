# OnPoint Technical Assessment - Data Pipeline POC

A mini data pipeline demonstrating crawling, ETL, storage, and CDC concepts.

## Project Structure
```
├── src/
│   ├── crawlers/
│   │   ├── api_crawler.py
│   │   └── web_crawler.py
│   ├── processors/
│   │   ├── data_processor.py
│   │   └── file_converter.py
│   ├── storage/
│   │   └── s3_uploader.py
│   ├── etl/
│   │   └── pipeline.py
│   └── utils/
│       └── config.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── outputs/
├── sql/
├── docs/
├── config/
├── requirements.txt
└── main.py
```

## Data Sources
- **API**: JSONPlaceholder (Posts and Users)
- **Web Scraping**: Hacker News front page

## Setup Instructions

### 1. Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration
Copy `config/config.example.json` to `config/config.json` and update AWS credentials if needed.

### 3. Run the Pipeline
```bash
python main.py
```

## Features Implemented

### ✅ Task 1: Crawling
- API crawler for JSONPlaceholder
- Web scraper for Hacker News
- Raw data saved as JSON files

### ✅ Task 2: Processing & File Formats
- Schema normalization with pandas
- Export to CSV, Parquet, and Excel
- S3 upload simulation (local folder)

### ✅ Task 3: ETL Pipeline
- Extract from JSON files
- Transform and clean data
- Generate PostgreSQL INSERT/UPSERT scripts

### ✅ Task 4: CDC & AWS DMS Documentation
- CDC streaming concepts
- AWS DMS configuration samples
- Bidirectional replication strategies

### ✅ Task 5: Orchestration & Scaling
- Airflow DAG design
- Scaling strategies for 100M+ records
- Performance optimization recommendations

## Architecture Overview

```
[APIs/Web] → [Crawlers] → [Raw JSON] → [Processors] → [Multiple Formats] → [S3/Storage]
                                    ↓
                               [ETL Pipeline] → [SQL Scripts] → [PostgreSQL]
                                    ↓
                               [CDC Stream] ↔ [AWS DMS] ↔ [S3 Data Lake]
```

## Technology Stack
- **Python 3.8+**: Core language
- **Pandas**: Data processing
- **BeautifulSoup4**: Web scraping
- **Requests**: API calls
- **PyArrow**: Parquet format
- **Boto3**: AWS S3 integration
- **SQLAlchemy**: SQL generation
- **Apache Airflow**: Orchestration (conceptual)

## Performance Considerations
- Async crawling for better performance
- Chunked processing for large datasets
- Parquet format for columnar efficiency
- Connection pooling for database operations

## Next Steps for Production
1. Implement proper error handling and retry logic
2. Add comprehensive logging and monitoring
3. Set up CI/CD pipeline
4. Implement data quality checks
5. Add authentication and security measures