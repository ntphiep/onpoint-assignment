"""
Main data pipeline runner for OnPoint Technical Assessment.
"""
import logging
import time
from pathlib import Path

import click

from src.crawlers.api_crawler import APICrawler
from src.crawlers.web_crawler import WebCrawler
from src.etl.pipeline import ETLPipeline
from src.processors.processor import FileConverter
from src.storage.s3_uploader import S3Uploader
from src.utils.config import config, setup_logging


logger = logging.getLogger(__name__)


@click.command()
@click.option('--skip-crawling', is_flag=True, help='Skip data crawling phase')
@click.option('--skip-processing', is_flag=True, help='Skip data processing phase')
@click.option('--skip-etl', is_flag=True, help='Skip ETL pipeline phase')
@click.option('--skip-upload', is_flag=True, help='Skip S3 upload phase')
@click.option('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
@click.option('--simulate-s3', is_flag=True, help='Simulate S3 uploads locally')
def main(skip_crawling, skip_processing, skip_etl, skip_upload, log_level, simulate_s3):
    """
    OnPoint Technical Assessment - Complete Data Pipeline
    
    This pipeline demonstrates:
    1. Data crawling from APIs and web pages
    2. Data processing and normalization  
    3. ETL pipeline with SQL generation
    4. File format conversion (CSV, Parquet, Excel)
    5. S3 upload simulation
    """
    setup_logging(log_level)
    
    logger.info("=" * 60)
    logger.info("OnPoint Technical Assessment - Data Pipeline")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    try:
        # Phase 1: Data Crawling
        if not skip_crawling:
            logger.info("\\n🕷️  PHASE 1: DATA CRAWLING")
            logger.info("-" * 40)
            
            # API Crawling
            logger.info("Starting API crawling...")
            api_crawler = APICrawler()
            api_crawler.run()
            
            # Web Scraping
            logger.info("Starting web scraping...")
            web_crawler = WebCrawler()
            web_crawler.run()
            
            logger.info("✅ Crawling phase completed")
        else:
            logger.info("⏭️  Skipping crawling phase")
        
        # Phase 2: Data Processing and File Conversion
        if not skip_processing:
            logger.info("\\n🔄 PHASE 2: DATA PROCESSING")
            logger.info("-" * 40)
            
            # Convert to multiple formats
            logger.info("Starting file format conversion...")
            file_converter = FileConverter()
            file_converter.run()
            
            logger.info("✅ Processing phase completed")
        else:
            logger.info("⏭️  Skipping processing phase")
        
        # Phase 3: ETL Pipeline
        if not skip_etl:
            logger.info("\\n🔧 PHASE 3: ETL PIPELINE")
            logger.info("-" * 40)
            
            # Run ETL pipeline
            logger.info("Starting ETL pipeline...")
            etl_pipeline = ETLPipeline()
            datasets = etl_pipeline.run_pipeline()
            
            logger.info("✅ ETL phase completed")
        else:
            logger.info("⏭️  Skipping ETL phase")
        
        # Phase 4: S3 Upload
        if not skip_upload:
            logger.info("\\n☁️  PHASE 4: S3 UPLOAD")
            logger.info("-" * 40)
            
            # Upload to S3 (or simulate)
            logger.info("Starting S3 upload...")
            s3_uploader = S3Uploader(simulate=simulate_s3)
            upload_results = s3_uploader.upload_processed_data()
            s3_uploader.create_upload_report()
            
            successful_uploads = sum(1 for success in upload_results.values() if success)
            total_files = len(upload_results)
            logger.info(f"Uploaded {successful_uploads}/{total_files} files")
            
            logger.info("✅ Upload phase completed")
        else:
            logger.info("⏭️  Skipping upload phase")
        
        # Final Summary
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("\\n🎉 PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Total execution time: {duration:.2f} seconds")
        logger.info(f"Configuration used: {Path(config.config_path).name}")
        
        # Print summary of outputs
        print_output_summary()
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        raise


def print_output_summary():
    """Print a summary of pipeline outputs."""
    print("\\n" + "=" * 60)
    print("PIPELINE OUTPUT SUMMARY")
    print("=" * 60)
    
    base_dir = Path(__file__).parent
    
    # Raw data
    raw_dir = base_dir / "data" / "raw"
    if raw_dir.exists():
        raw_files = list(raw_dir.glob("*.json"))
        print(f"\\n📁 Raw Data ({len(raw_files)} files):")
        for file in raw_files:
            size_kb = file.stat().st_size / 1024
            print(f"  • {file.name} ({size_kb:.1f} KB)")
    
    # Processed data
    processed_dir = base_dir / "data" / "processed"
    if processed_dir.exists():
        processed_files = list(processed_dir.glob("*.csv"))
        print(f"\\n📊 Processed Data ({len(processed_files)} files):")
        for file in processed_files:
            size_kb = file.stat().st_size / 1024
            print(f"  • {file.name} ({size_kb:.1f} KB)")
    
    # Output formats
    outputs_dir = base_dir / "data" / "outputs"
    if outputs_dir.exists():
        format_dirs = [d for d in outputs_dir.iterdir() if d.is_dir()]
        print(f"\\n📄 Output Formats ({len(format_dirs)} formats):")
        for format_dir in format_dirs:
            files = list(format_dir.glob("*"))
            files_count = len([f for f in files if f.is_file()])
            print(f"  • {format_dir.name}: {files_count} files")
    
    # SQL scripts
    sql_dir = base_dir / "sql"
    if sql_dir.exists():
        sql_files = list(sql_dir.glob("*.sql"))
        print(f"\\n🗄️  SQL Scripts ({len(sql_files)} files):")
        for file in sql_files:
            size_kb = file.stat().st_size / 1024
            print(f"  • {file.name} ({size_kb:.1f} KB)")
    
    # S3 simulation
    s3_sim_dir = base_dir / "data" / "s3_simulation"
    if s3_sim_dir.exists():
        s3_files = list(s3_sim_dir.rglob("*"))
        s3_files_count = len([f for f in s3_files if f.is_file() and not f.name.endswith('.metadata')])
        print(f"\\n☁️  S3 Simulation: {s3_files_count} files uploaded")
    
    print("\\n💡 Next Steps:")
    print("  1. Review the generated documentation in docs/")
    print("  2. Execute SQL scripts on PostgreSQL: cd sql && ./execute_etl.sh")
    print("  3. Check data quality and formats in data/outputs/")
    print("  4. Configure real AWS credentials for production S3 upload")
    print("=" * 60)


if __name__ == "__main__":
    main()