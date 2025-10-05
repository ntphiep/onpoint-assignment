# Orchestration & Scaling

This doc describes an Airflow DAG for the project and guidance to scale the pipeline to high volumes (100M+ records).

## Airflow DAG Design

- DAG name: `onpoint_pipeline`  
- Schedule: manual or cron (recommended: `@daily` or triggered on-demand).  
- Execution model: sequential phases with independent failure/retry policies. Each phase is idempotent so failures can be retried safely.

Tasks (logical order):
1. crawl_api (PythonOperator) – runs `src.crawlers.api_crawler.APICrawler.run()`
2. crawl_web (PythonOperator) – runs `src.crawlers.web_crawler.WebCrawler.run()`
3. process_files (PythonOperator) – runs `src.processors.processor.FileConverter.run()`
4. run_etl (PythonOperator) – runs `src.etl.pipeline.ETLPipeline.run_pipeline()`
5. upload_to_s3 (PythonOperator) – runs `src.storage.s3_uploader.S3Uploader.upload_processed_data()` with `simulate` flag from config
6. notify (Dummy/Email) – optional notification on success

Retries:
- Crawlers: retry 2 times with exponential backoff (network issues).  
- Processing: retry 1 time.  
- ETL: no automatic retry for idempotency reasons; use manual re-run if transforms changed.

Observability:
- Use Airflow logging and XCom for passing small payloads (counts, paths).  
- Emit metrics: number of raw files, processed rows, upload successes.

Idempotency & Re-runs:
- All pipeline tasks should be idempotent (replacing/overwriting outputs or writing to date-partitioned folders).  
- For `FileConverter`, outputs are deterministic file names in `data/processed`. Tasks that run again will overwrite.

## Airflow DAG File
- A sample DAG is included in `airflow/dags/onpoint_pipeline_dag.py` which calls project modules and reads `src/utils/config.py` for the simulate flag.

## Scaling to 100M+ records (high level)

1. Crawling
   - Parallelize crawlers across many workers or containers. Use rate limiting and backoff.
   - Use distributed crawling frameworks (Scrapy + Scrapyd) or serverless functions for API calls.
   - Persist raw JSON files directly to S3 rather than local disk to avoid I/O bottlenecks.

2. File Processing
   - Use Parquet as the primary analytical format (columnar, compressed) for large datasets.
   - Move from pandas to Dask or Spark for parallel, out-of-core processing.
   - Partition data by date or other logical keys to enable independent processing and incremental runs.
   - Use streaming/batch micro-batches: process incoming files in batches (e.g., hourly) rather than one-by-one.

3. ETL / CDC
   - For CDC at scale, prefer streaming platforms: Debezium → Kafka → Kinesis Firehose → S3 or Redshift.
   - If using DMS, scale replication instances and tune DMS settings; offload heavy transformation to Glue or EMR.

4. Storage & Querying
   - Use partitioned Parquet with Hive/Glue catalog for efficient reads.  
   - Consider using columnar stores (Redshift, BigQuery) for analytics that require fast query performance.

5. Orchestration
   - Use Airflow with Kubernetes Executor or CeleryExecutor to scale workers horizontally.
   - Break DAGs into smaller subDAGs or task groups and use sensors to pause/join flows.

6. Cost & Operational Considerations
   - Monitor S3 request & storage costs; compact small files (compaction) to reduce per-object overhead.
   - Implement data retention and lifecycle policies (glacier for older data).

## Quick Operational Checklist
- Run DAG with `simulate_s3=True` for testing.  
- Configure AWS credentials via `config/config.json` or environment variables for production.  
- Add Airflow variables and connections for db and S3 endpoints.

## Next Steps
- Add automated tests for ETL transforms.  
- Add a compaction job for S3 to reduce small files.  
- Add a schema registry / catalog for tracking schema evolution.
