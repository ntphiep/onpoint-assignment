# Orchestration & Scaling Strategy

## Overview

This document outlines the orchestration strategy using Apache Airflow and scaling approaches for handling 100M+ records in our data pipeline. It covers DAG design, performance optimization, and infrastructure scaling considerations.

## Table of Contents

1. [Airflow DAG Design](#airflow-dag-design)
2. [Scaling for 100M+ Records](#scaling-for-100m-records)
3. [Performance Optimization](#performance-optimization)
4. [Infrastructure Architecture](#infrastructure-architecture)
5. [Monitoring and Alerting](#monitoring-and-alerting)

---

## Airflow DAG Design

### Main Pipeline DAG

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# DAG definition
dag = DAG(
    'onpoint_data_pipeline',
    default_args=default_args,
    description='Complete data pipeline with crawling, processing, and loading',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['data-pipeline', 'etl', 'onpoint']
)

# Task definitions
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Crawling tasks (parallel)
crawl_apis = PythonOperator(
    task_id='crawl_apis',
    python_callable=run_api_crawling,
    dag=dag,
    pool='crawling_pool',
    retries=2
)

crawl_web = PythonOperator(
    task_id='crawl_web',
    python_callable=run_web_crawling,
    dag=dag,
    pool='crawling_pool',
    retries=2
)

# Data validation
validate_raw_data = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_crawled_data,
    dag=dag,
    trigger_rule='all_done'  # Continue even if some crawling fails
)

# Processing tasks
process_data = PythonOperator(
    task_id='process_data',
    python_callable=run_data_processing,
    dag=dag,
    pool='processing_pool'
)

convert_formats = PythonOperator(
    task_id='convert_formats',
    python_callable=run_format_conversion,
    dag=dag,
    pool='processing_pool'
)

# ETL pipeline
run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl_pipeline,
    dag=dag,
    pool='processing_pool'
)

# Database operations
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql='sql/create_tables.sql',
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_postgres,
    dag=dag,
    pool='database_pool'
)

# S3 upload
upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_processed_data,
    dag=dag,
    pool='s3_pool'
)

# Data quality checks
data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=run_data_quality_checks,
    dag=dag
)

# Cleanup
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='find /tmp/pipeline_{{ ds }} -type f -delete',
    dag=dag,
    trigger_rule='all_done'
)

# Success notification
send_success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_pipeline_notification,
    dag=dag,
    trigger_rule='all_success'
)

# Define task dependencies
start_pipeline >> [crawl_apis, crawl_web]
[crawl_apis, crawl_web] >> validate_raw_data
validate_raw_data >> process_data
process_data >> [convert_formats, run_etl]
[convert_formats, run_etl] >> create_tables
create_tables >> load_data
load_data >> [upload_to_s3, data_quality_check]
[upload_to_s3, data_quality_check] >> send_success_notification
send_success_notification >> cleanup_temp_files
```

### Supporting Functions

```python
import logging
from typing import Dict, Any
from airflow.models import Variable
from src.crawlers.api_crawler import APICrawler
from src.crawlers.web_crawler import WebCrawler
from src.processors.data_processor import DataProcessor
from src.processors.file_converter import FileConverter
from src.etl.pipeline import ETLPipeline
from src.storage.s3_uploader import S3Uploader

def run_api_crawling(**context):
    """Execute API crawling with error handling."""
    try:
        logging.info("Starting API crawling...")
        crawler = APICrawler()
        crawler.run()
        logging.info("API crawling completed successfully")
        return {"status": "success", "message": "API crawling completed"}
    except Exception as e:
        logging.error(f"API crawling failed: {e}")
        raise

def run_web_crawling(**context):
    """Execute web crawling with error handling."""
    try:
        logging.info("Starting web crawling...")
        crawler = WebCrawler()
        crawler.run()
        logging.info("Web crawling completed successfully")
        return {"status": "success", "message": "Web crawling completed"}
    except Exception as e:
        logging.error(f"Web crawling failed: {e}")
        raise

def validate_crawled_data(**context):
    """Validate that crawled data meets quality standards."""
    import os
    from pathlib import Path
    
    raw_data_dir = Path("/opt/airflow/data/raw")
    required_files = [
        "jsonplaceholder_posts.json",
        "jsonplaceholder_users.json", 
        "hackernews_stories.json"
    ]
    
    validation_results = {}
    for file_name in required_files:
        file_path = raw_data_dir / file_name
        if file_path.exists():
            file_size = file_path.stat().st_size
            validation_results[file_name] = {
                "exists": True,
                "size_bytes": file_size,
                "size_mb": round(file_size / 1024 / 1024, 2)
            }
        else:
            validation_results[file_name] = {"exists": False}
    
    # Check if we have minimum viable data
    valid_files = sum(1 for result in validation_results.values() if result.get("exists"))
    if valid_files < 2:
        raise ValueError(f"Insufficient data files. Only {valid_files} valid files found.")
    
    logging.info(f"Data validation completed: {validation_results}")
    return validation_results

def run_data_processing(**context):
    """Execute data processing pipeline."""
    try:
        logging.info("Starting data processing...")
        processor = DataProcessor()
        datasets = processor.process_all_data()
        
        # Store processing statistics
        stats = {name: {"rows": len(df), "columns": len(df.columns)} 
                for name, df in datasets.items()}
        
        logging.info(f"Data processing completed: {stats}")
        return {"status": "success", "datasets": stats}
    except Exception as e:
        logging.error(f"Data processing failed: {e}")
        raise

def run_format_conversion(**context):
    """Execute format conversion to multiple file types."""
    try:
        logging.info("Starting format conversion...")
        converter = FileConverter()
        converter.run()
        logging.info("Format conversion completed successfully")
        return {"status": "success", "message": "Format conversion completed"}
    except Exception as e:
        logging.error(f"Format conversion failed: {e}")
        raise

def run_etl_pipeline(**context):
    """Execute ETL pipeline and generate SQL scripts."""
    try:
        logging.info("Starting ETL pipeline...")
        etl = ETLPipeline()
        datasets = etl.run_pipeline()
        
        stats = {name: len(df) for name, df in datasets.items()}
        logging.info(f"ETL pipeline completed: {stats}")
        return {"status": "success", "datasets": stats}
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        raise

def load_data_to_postgres(**context):
    """Load processed data to PostgreSQL."""
    import subprocess
    
    try:
        logging.info("Loading data to PostgreSQL...")
        
        # Execute SQL scripts
        sql_dir = "/opt/airflow/sql"
        result = subprocess.run(
            ["bash", f"{sql_dir}/execute_etl.sh"],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode != 0:
            raise Exception(f"SQL execution failed: {result.stderr}")
        
        logging.info("Data loaded to PostgreSQL successfully")
        return {"status": "success", "message": "Data loaded successfully"}
    except Exception as e:
        logging.error(f"Database loading failed: {e}")
        raise

def upload_processed_data(**context):
    """Upload processed data to S3."""
    try:
        logging.info("Starting S3 upload...")
        uploader = S3Uploader()
        results = uploader.upload_processed_data()
        
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        logging.info(f"S3 upload completed: {successful}/{total} files")
        return {"status": "success", "uploaded_files": successful, "total_files": total}
    except Exception as e:
        logging.error(f"S3 upload failed: {e}")
        raise

def run_data_quality_checks(**context):
    """Run comprehensive data quality checks."""
    import psycopg2
    from airflow.hooks.postgres_hook import PostgresHook
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        quality_checks = [
            {
                "name": "row_count_check",
                "sql": "SELECT COUNT(*) as count FROM onpoint_posts",
                "expected_min": 1
            },
            {
                "name": "null_check",
                "sql": "SELECT COUNT(*) as nulls FROM onpoint_users WHERE email IS NULL",
                "expected_max": 0
            },
            {
                "name": "duplicate_check", 
                "sql": "SELECT COUNT(*) - COUNT(DISTINCT id) as duplicates FROM onpoint_posts",
                "expected_max": 0
            }
        ]
        
        results = {}
        for check in quality_checks:
            result = postgres_hook.get_first(check["sql"])
            value = result[0] if result else 0
            
            passed = True
            if "expected_min" in check:
                passed = passed and value >= check["expected_min"]
            if "expected_max" in check:
                passed = passed and value <= check["expected_max"]
            
            results[check["name"]] = {
                "value": value,
                "passed": passed
            }
        
        failed_checks = [name for name, result in results.items() if not result["passed"]]
        
        if failed_checks:
            raise ValueError(f"Data quality checks failed: {failed_checks}")
        
        logging.info(f"All data quality checks passed: {results}")
        return {"status": "success", "checks": results}
    
    except Exception as e:
        logging.error(f"Data quality checks failed: {e}")
        raise

def send_pipeline_notification(**context):
    """Send pipeline completion notification."""
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    
    # Get task instance info
    dag_run = context['dag_run']
    execution_date = context['execution_date']
    
    message = f"""
    ✅ OnPoint Data Pipeline Completed Successfully
    
    📊 Execution Details:
    • DAG: {dag_run.dag_id}
    • Execution Date: {execution_date}
    • Duration: {dag_run.end_date - dag_run.start_date if dag_run.end_date else 'Running'}
    
    📈 Pipeline Summary:
    • Data crawled from APIs and web sources
    • Processed and converted to multiple formats
    • Loaded to PostgreSQL and uploaded to S3
    • All data quality checks passed
    """
    
    logging.info("Pipeline completed successfully")
    # In production, send to Slack/email/etc.
    return {"status": "success", "message": message}
```

### Resource Pools Configuration

```python
# airflow.cfg or via UI
[pools]
crawling_pool = 4
processing_pool = 2
database_pool = 3
s3_pool = 2
```

---

## Scaling for 100M+ Records

### 1. Crawling Optimization

#### Distributed Crawling Architecture

```python
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio
import aiohttp

class ScalableCrawler:
    """Scalable crawler for handling large volumes."""
    
    def __init__(self, max_workers=50, batch_size=1000):
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(max_workers)
    
    async def crawl_large_dataset(self, urls):
        """Crawl large number of URLs efficiently."""
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            
            # Process URLs in batches
            tasks = []
            for i in range(0, len(urls), self.batch_size):
                batch = urls[i:i + self.batch_size]
                task = self.process_batch(session, batch)
                tasks.append(task)
            
            # Execute batches concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results
    
    async def process_batch(self, session, urls):
        """Process a batch of URLs."""
        async with self.semaphore:
            batch_tasks = [self.fetch_url(session, url) for url in urls]
            return await asyncio.gather(*batch_tasks, return_exceptions=True)
```

#### API Rate Limiting and Backoff

```python
import asyncio
import random
from typing import Optional

class RateLimitedCrawler:
    """Crawler with intelligent rate limiting."""
    
    def __init__(self, requests_per_second: int = 10):
        self.requests_per_second = requests_per_second
        self.request_interval = 1.0 / requests_per_second
        self.last_request_time = 0
        
    async def fetch_with_backoff(self, session, url: str, max_retries: int = 5):
        """Fetch URL with exponential backoff."""
        for attempt in range(max_retries):
            try:
                # Rate limiting
                now = time.time()
                time_since_last = now - self.last_request_time
                if time_since_last < self.request_interval:
                    await asyncio.sleep(self.request_interval - time_since_last)
                
                self.last_request_time = time.time()
                
                async with session.get(url) as response:
                    if response.status == 429:  # Rate limited
                        wait_time = 2 ** attempt + random.uniform(0, 1)
                        await asyncio.sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    return await response.json()
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                
                wait_time = 2 ** attempt + random.uniform(0, 1)
                await asyncio.sleep(wait_time)
```

### 2. File Processing Optimization

#### Chunked Processing with Dask

```python
import dask.dataframe as dd
import dask
from dask.distributed import Client

class ScalableProcessor:
    """Scalable data processor using Dask."""
    
    def __init__(self, n_workers=4, memory_limit='2GB'):
        self.client = Client(
            n_workers=n_workers,
            threads_per_worker=2,
            memory_limit=memory_limit
        )
    
    def process_large_dataset(self, file_pattern: str):
        """Process large datasets using Dask."""
        # Read files in parallel
        df = dd.read_json(file_pattern, lines=True)
        
        # Optimize chunk size based on available memory
        df = df.repartition(partition_size="100MB")
        
        # Distributed processing
        processed_df = df.map_partitions(
            self.process_partition,
            meta=df._meta
        )
        
        # Write to optimized format
        processed_df.to_parquet(
            'processed_data/',
            compression='snappy',
            engine='pyarrow'
        )
        
        return processed_df
    
    def process_partition(self, partition_df):
        """Process individual partition."""
        # Apply transformations
        partition_df = partition_df.fillna('')
        partition_df['processed_at'] = pd.Timestamp.now()
        
        # Data validation
        partition_df = partition_df[partition_df['id'].notna()]
        
        return partition_df
```

#### Parallel Format Conversion

```python
from multiprocessing import Pool
import pyarrow as pa
import pyarrow.parquet as pq

class ParallelFileConverter:
    """Convert files to multiple formats in parallel."""
    
    def __init__(self, n_processes=None):
        self.n_processes = n_processes or os.cpu_count()
    
    def convert_large_dataset(self, input_files):
        """Convert multiple large files in parallel."""
        with Pool(self.n_processes) as pool:
            # Map conversion tasks to processes
            results = pool.map(self.convert_file, input_files)
        
        return results
    
    def convert_file(self, file_info):
        """Convert single file to multiple formats."""
        input_path, output_dir = file_info
        
        # Read data
        df = pd.read_csv(input_path)
        
        # Convert to different formats
        formats = {
            'parquet': self.to_parquet,
            'excel': self.to_excel,
            'json': self.to_json
        }
        
        results = {}
        for format_name, converter in formats.items():
            try:
                output_path = f"{output_dir}/{Path(input_path).stem}.{format_name}"
                converter(df, output_path)
                results[format_name] = True
            except Exception as e:
                results[format_name] = False
                
        return results
    
    def to_parquet(self, df, output_path):
        """Convert to optimized Parquet."""
        # Optimize data types
        df = self.optimize_dtypes(df)
        
        # Write with optimal settings
        df.to_parquet(
            output_path,
            compression='snappy',
            engine='pyarrow',
            index=False
        )
    
    def optimize_dtypes(self, df):
        """Optimize data types for memory efficiency."""
        for col in df.columns:
            if df[col].dtype == 'object':
                # Try to convert to category for string columns
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:
                    df[col] = df[col].astype('category')
            elif df[col].dtype == 'int64':
                # Downcast integers
                df[col] = pd.to_numeric(df[col], downcast='integer')
            elif df[col].dtype == 'float64':
                # Downcast floats
                df[col] = pd.to_numeric(df[col], downcast='float')
        
        return df
```

### 3. CDC Replication Scaling

#### High-Throughput CDC Configuration

```json
{
  "ReplicationTaskSettings": {
    "TargetMetadata": {
      "ParallelLoadThreads": 16,
      "ParallelLoadBufferSize": 1000
    },
    "FullLoadSettings": {
      "MaxFullLoadSubTasks": 16,
      "ParallelLoadThreads": 16,
      "ParallelLoadBufferSize": 1000,
      "TransactionConsistencyTimeout": 3600,
      "CommitRate": 50000
    },
    "ChangeProcessingTuning": {
      "BatchApplyEnabled": true,
      "BatchApplyPreserveTransaction": false,
      "BatchSplitSize": 0,
      "BatchApplyTimeoutMin": 1,
      "BatchApplyTimeoutMax": 30,
      "BatchApplyMemoryLimit": 2000,
      "StatementCacheSize": 100,
      "CommitTimeout": 1,
      "MemoryLimitTotal": 4096,
      "MemoryKeepTime": 60
    },
    "StreamBufferSettings": {
      "StreamBufferCount": 10,
      "StreamBufferSizeInMB": 16,
      "CtrlStreamBufferSizeInMB": 8
    }
  }
}
```

#### Partitioned S3 Output Strategy

```python
class ScalableS3Output:
    """Scalable S3 output with intelligent partitioning."""
    
    def __init__(self):
        self.partition_strategy = {
            "time_based": ["year", "month", "day", "hour"],
            "size_based": {"max_file_size_mb": 128},
            "columnar_optimization": True
        }
    
    def configure_s3_target(self, table_name: str):
        """Configure S3 target for high-volume data."""
        return {
            "BucketName": "data-lake-production",
            "BucketFolder": f"cdc/{table_name}",
            "DataFormat": "parquet",
            "CompressionType": "gzip",
            "EnableStatistics": True,
            "IncludeOpForFullLoad": True,
            "TimestampColumnName": "cdc_timestamp",
            "DatePartitionEnabled": True,
            "DatePartitionSequence": "YYYYMMDDHH",
            "ParquetTimestampInMillisecond": True,
            "ParquetVersion": "parquet-2-0",
            "MaxFileSize": 134217728,  # 128MB
            "CdcMaxBatchInterval": 60,  # 1 minute
            "CdcMinFileSize": 67108864  # 64MB
        }
```

---

## Performance Optimization

### 1. Memory Management

```python
import gc
import psutil
from memory_profiler import profile

class MemoryOptimizedPipeline:
    """Pipeline with intelligent memory management."""
    
    def __init__(self, memory_threshold=0.8):
        self.memory_threshold = memory_threshold
        self.initial_memory = psutil.virtual_memory().available
    
    def check_memory_usage(self):
        """Monitor memory usage and trigger cleanup."""
        memory_percent = psutil.virtual_memory().percent / 100
        
        if memory_percent > self.memory_threshold:
            logging.warning(f"High memory usage: {memory_percent:.1%}")
            self.cleanup_memory()
    
    def cleanup_memory(self):
        """Force garbage collection and cleanup."""
        gc.collect()
        
        # Clear pandas caches
        pd.options.mode.chained_assignment = None
        
        logging.info("Memory cleanup completed")
    
    @profile
    def process_with_memory_monitoring(self, data):
        """Process data with memory monitoring."""
        self.check_memory_usage()
        
        # Process in chunks
        chunk_size = self.calculate_optimal_chunk_size()
        
        for chunk in self.chunked_processing(data, chunk_size):
            yield self.process_chunk(chunk)
            self.check_memory_usage()
    
    def calculate_optimal_chunk_size(self):
        """Calculate optimal chunk size based on available memory."""
        available_memory = psutil.virtual_memory().available
        
        # Use 25% of available memory for chunk processing
        chunk_memory = available_memory * 0.25
        
        # Estimate rows per chunk (rough estimate: 1KB per row)
        estimated_chunk_size = int(chunk_memory / 1024)
        
        # Ensure minimum and maximum bounds
        return max(1000, min(estimated_chunk_size, 100000))
```

### 2. Database Connection Optimization

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import psycopg2.extras

class OptimizedDatabaseManager:
    """Optimized database connection management."""
    
    def __init__(self):
        self.engine = create_engine(
            'postgresql://user:pass@host:port/db',
            poolclass=QueuePool,
            pool_size=20,
            max_overflow=30,
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={
                'options': '-c statement_timeout=300000',  # 5 minutes
                'application_name': 'onpoint_pipeline'
            }
        )
    
    def bulk_insert_optimized(self, df, table_name):
        """Optimized bulk insert using COPY."""
        with self.engine.connect() as conn:
            # Use COPY for maximum performance
            output = io.StringIO()
            df.to_csv(output, sep='\\t', header=False, index=False, na_rep='\\\\N')
            output.seek(0)
            
            raw_conn = conn.connection.connection
            cursor = raw_conn.cursor()
            
            try:
                cursor.copy_from(
                    output,
                    table_name,
                    columns=list(df.columns),
                    sep='\\t',
                    null='\\\\N'
                )
                raw_conn.commit()
            except Exception as e:
                raw_conn.rollback()
                raise
            finally:
                cursor.close()
    
    def parallel_processing(self, queries, max_workers=5):
        """Execute multiple queries in parallel."""
        from concurrent.futures import ThreadPoolExecutor
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.execute_query, query) for query in queries]
            results = [future.result() for future in futures]
        
        return results
```

### 3. File I/O Optimization

```python
class OptimizedFileHandler:
    """Optimized file I/O operations."""
    
    def __init__(self):
        self.compression_settings = {
            'parquet': {'compression': 'snappy', 'use_dictionary': True},
            'csv': {'compression': 'gzip', 'chunksize': 10000},
            'json': {'compression': 'gzip', 'orient': 'records'}
        }
    
    def read_large_file_streaming(self, file_path, format='csv'):
        """Stream large files for memory efficiency."""
        if format == 'csv':
            chunk_iter = pd.read_csv(
                file_path,
                chunksize=self.compression_settings['csv']['chunksize'],
                compression='infer'
            )
            for chunk in chunk_iter:
                yield chunk
        
        elif format == 'parquet':
            # Read Parquet in row groups
            parquet_file = pq.ParquetFile(file_path)
            for batch in parquet_file.iter_batches(batch_size=10000):
                yield batch.to_pandas()
    
    def write_optimized_parquet(self, df, output_path):
        """Write Parquet with optimal settings."""
        # Optimize schema
        schema = self.optimize_parquet_schema(df)
        
        # Write with optimal settings
        df.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy',
            index=False,
            partition_cols=None,
            schema=schema,
            use_dictionary=True,
            row_group_size=100000,
            flavor='spark'
        )
    
    def optimize_parquet_schema(self, df):
        """Create optimized Parquet schema."""
        import pyarrow as pa
        
        schema_fields = []
        for col, dtype in df.dtypes.items():
            if dtype == 'object':
                # String columns
                pa_type = pa.string()
            elif dtype.name.startswith('int'):
                # Integer columns
                pa_type = pa.int64()
            elif dtype.name.startswith('float'):
                # Float columns
                pa_type = pa.float64()
            elif dtype.name.startswith('bool'):
                # Boolean columns
                pa_type = pa.bool_()
            elif dtype.name.startswith('datetime'):
                # Timestamp columns
                pa_type = pa.timestamp('ns')
            else:
                pa_type = pa.string()
            
            schema_fields.append((col, pa_type))
        
        return pa.schema(schema_fields)
```

---

## Infrastructure Architecture

### 1. Kubernetes Deployment for Airflow

```yaml
# airflow-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: data-pipeline
spec:
  replicas: 2
  selector:
    matchLabels:
      app: airflow-webserver
  template:
    metadata:
      labels:
        app: airflow-webserver
    spec:
      containers:
      - name: airflow-webserver
        image: apache/airflow:2.5.0
        ports:
        - containerPort: 8080
        env:
        - name: AIRFLOW__CORE__EXECUTOR
          value: "CeleryExecutor"
        - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
          valueFrom:
            secretKeyRef:
              name: airflow-secrets
              key: sql_alchemy_conn
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        volumeMounts:
        - name: airflow-dags
          mountPath: /opt/airflow/dags
        - name: airflow-data
          mountPath: /opt/airflow/data
      volumes:
      - name: airflow-dags
        configMap:
          name: airflow-dags
      - name: airflow-data
        persistentVolumeClaim:
          claimName: airflow-data-pvc

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-worker
  namespace: data-pipeline
spec:
  replicas: 4
  selector:
    matchLabels:
      app: airflow-worker
  template:
    metadata:
      labels:
        app: airflow-worker
    spec:
      containers:
      - name: airflow-worker
        image: apache/airflow:2.5.0
        command: ["airflow", "celery", "worker"]
        env:
        - name: AIRFLOW__CORE__EXECUTOR
          value: "CeleryExecutor"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2000m"
          limits:
            memory: "8Gi"
            cpu: "4000m"
        volumeMounts:
        - name: airflow-data
          mountPath: /opt/airflow/data
      volumes:
      - name: airflow-data
        persistentVolumeClaim:
          claimName: airflow-data-pvc
```

### 2. Auto-scaling Configuration

```yaml
# horizontal-pod-autoscaler.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-worker-hpa
  namespace: data-pipeline
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 60
```

### 3. Infrastructure as Code with Terraform

```hcl
# main.tf
provider "aws" {
  region = var.aws_region
}

# EKS Cluster for Airflow
module "eks" {
  source = "terraform-aws-modules/eks/aws"
  
  cluster_name    = "onpoint-data-pipeline"
  cluster_version = "1.24"
  
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets
  
  node_groups = {
    airflow_workers = {
      desired_capacity = 4
      max_capacity     = 20
      min_capacity     = 2
      
      instance_types = ["m5.2xlarge"]
      
      k8s_labels = {
        Environment = "production"
        Application = "airflow"
      }
      
      additional_tags = {
        "kubernetes.io/cluster/onpoint-data-pipeline" = "owned"
      }
    }
  }
}

# RDS for Airflow Metadata
resource "aws_db_instance" "airflow_metadata" {
  identifier = "airflow-metadata"
  
  engine         = "postgres"
  engine_version = "14.6"
  instance_class = "db.r5.2xlarge"
  
  allocated_storage     = 100
  max_allocated_storage = 1000
  storage_type         = "gp2"
  storage_encrypted    = true
  
  db_name  = "airflow"
  username = "airflow"
  password = var.db_password
  
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.airflow.name
  
  backup_retention_period = 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  performance_insights_enabled = true
  monitoring_interval         = 60
  
  tags = {
    Name = "airflow-metadata"
  }
}

# ElastiCache for Celery
resource "aws_elasticache_cluster" "celery_broker" {
  cluster_id           = "airflow-celery"
  engine              = "redis"
  node_type           = "cache.r5.xlarge"
  num_cache_nodes     = 1
  parameter_group_name = "default.redis6.x"
  port                = 6379
  subnet_group_name   = aws_elasticache_subnet_group.airflow.name
  security_group_ids  = [aws_security_group.elasticache.id]
}

# S3 Bucket for Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "onpoint-data-lake-${random_id.bucket_suffix.hex}"
  
  tags = {
    Name = "OnPoint Data Lake"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  rule {
    id     = "transition_to_ia"
    status = "Enabled"
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
  }
}
```

---

## Monitoring and Alerting

### 1. Comprehensive Monitoring Setup

```python
# monitoring.py
import logging
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def monitor_pipeline_health(**context):
    """Comprehensive pipeline health monitoring."""
    
    health_checks = {
        'database_connectivity': check_database_health,
        'data_freshness': check_data_freshness,
        'processing_latency': check_processing_latency,
        'error_rates': check_error_rates,
        'resource_utilization': check_resource_utilization
    }
    
    results = {}
    for check_name, check_function in health_checks.items():
        try:
            results[check_name] = check_function()
        except Exception as e:
            results[check_name] = {
                'status': 'FAILED',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    # Determine overall health
    failed_checks = [name for name, result in results.items() 
                    if result.get('status') == 'FAILED']
    
    overall_status = 'HEALTHY' if not failed_checks else 'UNHEALTHY'
    
    # Send alerts if unhealthy
    if overall_status == 'UNHEALTHY':
        send_health_alert(failed_checks, results)
    
    return {
        'overall_status': overall_status,
        'failed_checks': failed_checks,
        'detailed_results': results,
        'timestamp': datetime.now().isoformat()
    }

def check_database_health():
    """Check database connectivity and performance."""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    start_time = time.time()
    
    # Test connectivity
    result = postgres_hook.get_first("SELECT 1")
    
    # Check response time
    response_time = time.time() - start_time
    
    # Check active connections
    active_connections = postgres_hook.get_first("""
        SELECT count(*) FROM pg_stat_activity 
        WHERE state = 'active' AND application_name = 'onpoint_pipeline'
    """)[0]
    
    # Check database size
    db_size = postgres_hook.get_first("""
        SELECT pg_size_pretty(pg_database_size(current_database()))
    """)[0]
    
    status = 'HEALTHY' if response_time < 5.0 and active_connections < 50 else 'WARNING'
    
    return {
        'status': status,
        'response_time_seconds': response_time,
        'active_connections': active_connections,
        'database_size': db_size,
        'timestamp': datetime.now().isoformat()
    }

def check_data_freshness():
    """Check if data is fresh and up-to-date."""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check latest data timestamps
    freshness_checks = [
        ('onpoint_posts', 'etl_processed_at'),
        ('onpoint_users', 'etl_processed_at'),
        ('onpoint_comments', 'etl_processed_at')
    ]
    
    results = {}
    for table, timestamp_col in freshness_checks:
        latest_timestamp = postgres_hook.get_first(f"""
            SELECT MAX({timestamp_col}) FROM {table}
        """)[0]
        
        if latest_timestamp:
            age_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
            status = 'HEALTHY' if age_hours < 25 else 'STALE'  # Within 25 hours (daily + buffer)
        else:
            age_hours = float('inf')
            status = 'NO_DATA'
        
        results[table] = {
            'latest_timestamp': latest_timestamp.isoformat() if latest_timestamp else None,
            'age_hours': age_hours,
            'status': status
        }
    
    overall_status = 'HEALTHY' if all(r['status'] == 'HEALTHY' for r in results.values()) else 'STALE'
    
    return {
        'status': overall_status,
        'tables': results,
        'timestamp': datetime.now().isoformat()
    }

def check_processing_latency():
    """Check processing latency and throughput."""
    # This would integrate with your actual metrics system
    # For demo purposes, returning mock data
    
    return {
        'status': 'HEALTHY',
        'avg_processing_time_minutes': 45,
        'records_processed_last_hour': 125000,
        'current_queue_size': 0,
        'timestamp': datetime.now().isoformat()
    }

def check_error_rates():
    """Check error rates from logs."""
    # This would integrate with CloudWatch, ELK, or similar
    # For demo purposes, returning mock data
    
    return {
        'status': 'HEALTHY',
        'error_rate_last_hour': 0.02,  # 2% error rate
        'critical_errors': 0,
        'warning_errors': 3,
        'timestamp': datetime.now().isoformat()
    }

def check_resource_utilization():
    """Check system resource utilization."""
    # This would integrate with Kubernetes metrics or CloudWatch
    # For demo purposes, returning mock data
    
    return {
        'status': 'HEALTHY',
        'cpu_utilization_percent': 65,
        'memory_utilization_percent': 70,
        'disk_utilization_percent': 45,
        'active_workers': 4,
        'timestamp': datetime.now().isoformat()
    }

def send_health_alert(failed_checks, results):
    """Send alert for health check failures."""
    message = f"""
    🚨 PIPELINE HEALTH ALERT 🚨
    
    Failed Checks: {', '.join(failed_checks)}
    
    Details:
    {json.dumps(results, indent=2)}
    
    Time: {datetime.now().isoformat()}
    """
    
    # In production, this would send to Slack, PagerDuty, etc.
    logging.error(f"Pipeline health alert: {message}")

# Monitoring DAG
monitoring_dag = DAG(
    'pipeline_monitoring',
    default_args={
        'owner': 'data-engineering',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline health monitoring',
    schedule_interval=timedelta(minutes=15),  # Every 15 minutes
    catchup=False,
    tags=['monitoring', 'health-check']
)

health_check_task = PythonOperator(
    task_id='pipeline_health_check',
    python_callable=monitor_pipeline_health,
    dag=monitoring_dag
)
```

### 2. Performance Metrics Dashboard

```python
# metrics_collector.py
from dataclasses import dataclass
from typing import Dict, List
import psutil
import time

@dataclass
class PipelineMetrics:
    """Pipeline performance metrics."""
    timestamp: str
    processing_time_seconds: float
    records_processed: int
    memory_usage_mb: float
    cpu_usage_percent: float
    error_count: int
    success_rate_percent: float

class MetricsCollector:
    """Collect and export pipeline metrics."""
    
    def __init__(self):
        self.metrics_history: List[PipelineMetrics] = []
        self.start_time = time.time()
    
    def collect_metrics(self, **kwargs) -> PipelineMetrics:
        """Collect current metrics."""
        current_time = time.time()
        
        metrics = PipelineMetrics(
            timestamp=datetime.now().isoformat(),
            processing_time_seconds=current_time - self.start_time,
            records_processed=kwargs.get('records_processed', 0),
            memory_usage_mb=psutil.virtual_memory().used / 1024 / 1024,
            cpu_usage_percent=psutil.cpu_percent(),
            error_count=kwargs.get('error_count', 0),
            success_rate_percent=kwargs.get('success_rate', 100.0)
        )
        
        self.metrics_history.append(metrics)
        return metrics
    
    def export_to_cloudwatch(self, metrics: PipelineMetrics):
        """Export metrics to CloudWatch."""
        import boto3
        
        cloudwatch = boto3.client('cloudwatch')
        
        metric_data = [
            {
                'MetricName': 'ProcessingTime',
                'Value': metrics.processing_time_seconds,
                'Unit': 'Seconds',
                'Dimensions': [{'Name': 'Pipeline', 'Value': 'OnPointETL'}]
            },
            {
                'MetricName': 'RecordsProcessed',
                'Value': metrics.records_processed,
                'Unit': 'Count',
                'Dimensions': [{'Name': 'Pipeline', 'Value': 'OnPointETL'}]
            },
            {
                'MetricName': 'MemoryUsage',
                'Value': metrics.memory_usage_mb,
                'Unit': 'Megabytes',
                'Dimensions': [{'Name': 'Pipeline', 'Value': 'OnPointETL'}]
            },
            {
                'MetricName': 'SuccessRate',
                'Value': metrics.success_rate_percent,
                'Unit': 'Percent',
                'Dimensions': [{'Name': 'Pipeline', 'Value': 'OnPointETL'}]
            }
        ]
        
        cloudwatch.put_metric_data(
            Namespace='OnPoint/DataPipeline',
            MetricData=metric_data
        )
```

---

## Conclusion

This orchestration and scaling strategy provides:

1. **Robust Airflow DAG** with proper error handling, monitoring, and notifications
2. **Scalable architecture** capable of handling 100M+ records through:
   - Distributed processing with Dask
   - Parallel crawling and format conversion
   - Optimized database operations
   - Intelligent memory management
3. **Production-ready infrastructure** with Kubernetes, auto-scaling, and IaC
4. **Comprehensive monitoring** with health checks, metrics collection, and alerting

The solution scales horizontally through additional workers and vertically through resource optimization, ensuring reliable performance at enterprise scale.