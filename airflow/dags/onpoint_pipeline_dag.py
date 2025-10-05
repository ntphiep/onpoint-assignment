from __future__ import annotations

"""Airflow DAG for the OnPoint pipeline.

This DAG calls into the project's Python entrypoints (crawlers, processor, ETL, uploader).
It is intended for local development and testing (default: simulate S3 uploads).
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_api_crawler(**kwargs):
    from src.crawlers.api_crawler import APICrawler
    c = APICrawler()
    c.run()


def _run_web_crawler(**kwargs):
    from src.crawlers.web_crawler import WebCrawler
    c = WebCrawler()
    c.run()


def _run_processor(**kwargs):
    from src.processors.processor import FileConverter
    import asyncio

    fc = FileConverter()
    # run the async runner
    asyncio.run(fc.run())


def _run_etl(**kwargs):
    from src.etl.pipeline import ETLPipeline
    p = ETLPipeline()
    p.run_pipeline()


def _run_upload(**kwargs):
    # Read simulate flag from project config
    from src.utils.config import config
    simulate = config.get('processing.simulate_s3', True)
    try:
        # prefer storage module if present
        from src.storage.s3_uploader import S3Uploader
    except Exception:
        from src.processors.s3_upload import S3Uploader

    base = S3Uploader.__init__.__annotations__.get('processed_dir')
    uploader = S3Uploader(processed_dir=None, bucket=None, region=None, access_key=None, secret_key=None, simulate=simulate)
    uploader.upload_processed_data()


default_args = {
    'owner': 'onpoint',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='onpoint_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    task_crawl_api = PythonOperator(
        task_id='crawl_api',
        python_callable=_run_api_crawler,
    )

    task_crawl_web = PythonOperator(
        task_id='crawl_web',
        python_callable=_run_web_crawler,
    )

    task_process = PythonOperator(
        task_id='process_files',
        python_callable=_run_processor,
    )

    task_etl = PythonOperator(
        task_id='run_etl',
        python_callable=_run_etl,
    )

    task_upload = PythonOperator(
        task_id='upload_to_s3',
        python_callable=_run_upload,
    )

    # linear flow: crawl -> process -> etl -> upload
    task_crawl_api >> task_crawl_web >> task_process >> task_etl >> task_upload
