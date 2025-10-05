"""
ETL Pipeline for extracting, transforming, and loading data.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from ..processors.data_processor import DataProcessor
from ..utils.config import config

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Complete ETL pipeline for data processing and SQL generation."""
    
    def __init__(self, sql_output_dir: str = None):
        """Initialize ETL pipeline."""
        self.sql_output_dir = sql_output_dir or str(Path(__file__).parent.parent.parent / "sql")
        Path(self.sql_output_dir).mkdir(parents=True, exist_ok=True)
        
        self.db_config = config.get_database_config()
        self.processor = DataProcessor()
    
    def extract(self) -> Dict[str, pd.DataFrame]:
        """Extract data from raw JSON files and process."""
        logger.info("Starting data extraction and processing...")
        return self.processor.process_all_data()
    
    def transform(self, datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform data for database insertion."""
        logger.info("Starting data transformation...")
        
        transformed_datasets = {}
        
        for name, df in datasets.items():
            logger.info(f"Transforming {name} dataset...")
            
            # Make a copy to avoid modifying original
            transformed_df = df.copy()
            
            # Common transformations
            transformed_df = self._apply_common_transformations(transformed_df, name)
            
            # Dataset-specific transformations
            if name == 'posts':
                transformed_df = self._transform_posts(transformed_df)
            elif name == 'users':
                transformed_df = self._transform_users(transformed_df)
            elif name == 'comments':
                transformed_df = self._transform_comments(transformed_df)
            elif name == 'hackernews_stories':
                transformed_df = self._transform_hackernews_stories(transformed_df)
            elif name == 'reddit_posts':
                transformed_df = self._transform_reddit_posts(transformed_df)
            
            transformed_datasets[name] = transformed_df
            logger.info(f"Transformed {name}: {transformed_df.shape}")
        
        return transformed_datasets
    
    def _apply_common_transformations(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
        """Apply common transformations to all datasets."""
        # Add ETL metadata
        df['etl_processed_at'] = datetime.now().isoformat()
        df['etl_dataset_name'] = dataset_name
        
        # Handle null values
        df = df.fillna('')
        
        # Clean string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                # Replace empty strings with None for proper SQL handling
                df[col] = df[col].replace('', None)
        
        return df
    
    def _transform_posts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform posts dataset for database."""
        # Ensure proper data types
        df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
        df['userId'] = pd.to_numeric(df['userId'], errors='coerce').astype('Int64')
        df['title_length'] = pd.to_numeric(df['title_length'], errors='coerce').astype('Int64')
        df['body_length'] = pd.to_numeric(df['body_length'], errors='coerce').astype('Int64')
        df['word_count'] = pd.to_numeric(df['word_count'], errors='coerce').astype('Int64')
        
        # Validate required fields
        df = df.dropna(subset=['id'])
        
        return df
    
    def _transform_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform users dataset for database."""
        # Ensure proper data types
        df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
        df['has_website'] = df['has_website'].astype(bool)
        
        # Validate email format (basic)
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
        df['email_valid'] = df['email'].str.match(email_pattern, na=False)
        
        # Clean phone numbers
        if 'phone' in df.columns:
            df['phone_cleaned'] = df['phone'].str.replace(r'[^0-9x.-]', '', regex=True)
        
        # Validate required fields
        df = df.dropna(subset=['id'])
        
        return df
    
    def _transform_comments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform comments dataset for database."""
        # Ensure proper data types
        df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
        df['postId'] = pd.to_numeric(df['postId'], errors='coerce').astype('Int64')
        df['body_length'] = pd.to_numeric(df['body_length'], errors='coerce').astype('Int64')
        df['word_count'] = pd.to_numeric(df['word_count'], errors='coerce').astype('Int64')
        
        # Validate required fields
        df = df.dropna(subset=['id', 'postId'])
        
        return df
    
    def _transform_hackernews_stories(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Hacker News stories for database."""
        # Ensure proper data types
        df['rank'] = pd.to_numeric(df['rank'], errors='coerce').astype('Int64')
        df['score_numeric'] = pd.to_numeric(df['score_numeric'], errors='coerce')
        df['comments_numeric'] = pd.to_numeric(df['comments_numeric'], errors='coerce').astype('Int64')
        df['title_length'] = pd.to_numeric(df['title_length'], errors='coerce').astype('Int64')
        df['has_external_url'] = df['has_external_url'].astype(bool)
        
        # Clean URLs
        df['url'] = df['url'].str.strip()
        df.loc[df['url'].str.startswith('item?id='), 'url'] = 'https://news.ycombinator.com/' + df['url']
        
        return df
    
    def _transform_reddit_posts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Reddit posts for database."""
        # Ensure proper data types
        df['score'] = pd.to_numeric(df['score'], errors='coerce').astype('Int64')
        df['num_comments'] = pd.to_numeric(df['num_comments'], errors='coerce').astype('Int64')
        df['title_length'] = pd.to_numeric(df['title_length'], errors='coerce').astype('Int64')
        df['selftext_length'] = pd.to_numeric(df['selftext_length'], errors='coerce').astype('Int64')
        df['has_selftext'] = df['has_selftext'].astype(bool)
        
        # Handle datetime
        if 'created_datetime' in df.columns:
            df['created_datetime'] = pd.to_datetime(df['created_datetime'], errors='coerce')
        
        return df
    
    def generate_table_ddl(self, df: pd.DataFrame, table_name: str) -> str:
        """Generate PostgreSQL table DDL from DataFrame."""
        ddl_lines = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
        
        # Map pandas dtypes to PostgreSQL types
        type_mapping = {
            'object': 'TEXT',
            'int64': 'BIGINT',
            'Int64': 'BIGINT',
            'float64': 'DECIMAL(10,2)',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'datetime64[ns, UTC]': 'TIMESTAMP WITH TIME ZONE'
        }
        
        column_definitions = []
        for col, dtype in df.dtypes.items():
            pg_type = type_mapping.get(str(dtype), 'TEXT')
            
            # Special cases
            if col.endswith('_id') or col == 'id':
                if 'id' in col.lower():
                    pg_type = 'BIGINT'
            elif 'email' in col.lower():
                pg_type = 'VARCHAR(255)'
            elif 'url' in col.lower():
                pg_type = 'TEXT'
            elif 'length' in col.lower() or 'count' in col.lower():
                pg_type = 'INTEGER'
            
            column_definitions.append(f"    {col} {pg_type}")
        
        ddl_lines.extend(column_definitions)
        ddl_lines.append(");")
        
        return "\\n".join(ddl_lines)
    
    def generate_insert_sql(self, df: pd.DataFrame, table_name: str) -> str:
        """Generate INSERT statements for DataFrame."""
        if df.empty:
            return f"-- No data to insert for table {table_name}\\n"
        
        sql_lines = [f"-- INSERT statements for {table_name}"]
        sql_lines.append(f"-- Total records: {len(df)}")
        sql_lines.append("")
        
        # Generate INSERT statements in batches
        batch_size = config.get('processing.batch_size', 1000)
        
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            
            sql_lines.append(f"-- Batch {i // batch_size + 1}")
            
            # Generate multi-row INSERT
            columns = list(batch_df.columns)
            column_list = ", ".join(columns)
            
            values_list = []
            for _, row in batch_df.iterrows():
                row_values = []
                for col in columns:
                    value = row[col]
                    if pd.isna(value) or value is None:
                        row_values.append("NULL")
                    elif isinstance(value, str):
                        # Escape single quotes
                        escaped_value = value.replace("'", "''")
                        row_values.append(f"'{escaped_value}'")
                    elif isinstance(value, bool):
                        row_values.append("TRUE" if value else "FALSE")
                    else:
                        row_values.append(str(value))
                
                values_list.append(f"    ({', '.join(row_values)})")
            
            insert_sql = f"""INSERT INTO {table_name} ({column_list})
VALUES
{',\\n'.join(values_list)};"""
            
            sql_lines.append(insert_sql)
            sql_lines.append("")
        
        return "\\n".join(sql_lines)
    
    def generate_upsert_sql(self, df: pd.DataFrame, table_name: str, conflict_column: str = 'id') -> str:
        """Generate UPSERT (INSERT ... ON CONFLICT) statements."""
        if df.empty:
            return f"-- No data to upsert for table {table_name}\\n"
        
        sql_lines = [f"-- UPSERT statements for {table_name}"]
        sql_lines.append(f"-- Conflict resolution on: {conflict_column}")
        sql_lines.append(f"-- Total records: {len(df)}")
        sql_lines.append("")
        
        columns = list(df.columns)
        column_list = ", ".join(columns)
        
        # Generate update clause for conflict resolution
        update_columns = [col for col in columns if col != conflict_column]
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        # Generate in smaller batches for upsert
        batch_size = 500
        
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            
            sql_lines.append(f"-- Upsert batch {i // batch_size + 1}")
            
            values_list = []
            for _, row in batch_df.iterrows():
                row_values = []
                for col in columns:
                    value = row[col]
                    if pd.isna(value) or value is None:
                        row_values.append("NULL")
                    elif isinstance(value, str):
                        escaped_value = value.replace("'", "''")
                        row_values.append(f"'{escaped_value}'")
                    elif isinstance(value, bool):
                        row_values.append("TRUE" if value else "FALSE")
                    else:
                        row_values.append(str(value))
                
                values_list.append(f"    ({', '.join(row_values)})")
            
            upsert_sql = f"""INSERT INTO {table_name} ({column_list})
VALUES
{',\\n'.join(values_list)}
ON CONFLICT ({conflict_column}) DO UPDATE SET
    {update_clause};"""
            
            sql_lines.append(upsert_sql)
            sql_lines.append("")
        
        return "\\n".join(sql_lines)
    
    def load(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """Load (generate SQL scripts) for transformed data."""
        logger.info("Starting SQL generation (Load phase)...")
        
        sql_dir = Path(self.sql_output_dir)
        
        # Generate master SQL file
        master_sql_lines = [
            "-- OnPoint Technical Assessment - ETL Pipeline SQL Scripts",
            f"-- Generated on: {datetime.now().isoformat()}",
            "-- Database: PostgreSQL",
            "",
            "-- Set session parameters",
            "SET client_encoding = 'UTF8';",
            "SET timezone = 'UTC';",
            "",
        ]
        
        for name, df in datasets.items():
            logger.info(f"Generating SQL for {name}...")
            
            table_name = f"onpoint_{name}"
            
            # Generate DDL
            ddl_sql = self.generate_table_ddl(df, table_name)
            
            # Generate INSERT statements
            insert_sql = self.generate_insert_sql(df, table_name)
            
            # Generate UPSERT statements (if applicable)
            upsert_sql = ""
            if 'id' in df.columns:
                upsert_sql = self.generate_upsert_sql(df, table_name, 'id')
            
            # Save individual SQL files
            # DDL file
            ddl_file = sql_dir / f"{name}_ddl.sql"
            with open(ddl_file, 'w', encoding='utf-8') as f:
                f.write(ddl_sql)
            
            # INSERT file
            insert_file = sql_dir / f"{name}_insert.sql"
            with open(insert_file, 'w', encoding='utf-8') as f:
                f.write(insert_sql)
            
            # UPSERT file (if applicable)
            if upsert_sql:
                upsert_file = sql_dir / f"{name}_upsert.sql"
                with open(upsert_file, 'w', encoding='utf-8') as f:
                    f.write(upsert_sql)
            
            logger.info(f"Generated SQL files for {name}")
            
            # Add to master file
            master_sql_lines.extend([
                f"-- {name.upper()} TABLE",
                "-- " + "=" * 50,
                ddl_sql,
                "",
                insert_sql,
                "",
            ])
        
        # Save master SQL file
        master_file = sql_dir / "complete_etl_pipeline.sql"
        with open(master_file, 'w', encoding='utf-8') as f:
            f.write("\\n".join(master_sql_lines))
        
        logger.info(f"Generated master SQL file: {master_file}")
        
        # Create execution script
        self.create_execution_script(list(datasets.keys()))
    
    def create_execution_script(self, dataset_names: List[str]) -> None:
        """Create a script to execute all SQL files in order."""
        script_content = [
            "#!/bin/bash",
            "# OnPoint ETL Pipeline - SQL Execution Script",
            "",
            "# Database connection parameters",
            f"DB_HOST={self.db_config.get('postgresql', {}).get('host', 'localhost')}",
            f"DB_PORT={self.db_config.get('postgresql', {}).get('port', 5432)}",
            f"DB_NAME={self.db_config.get('postgresql', {}).get('database', 'onpoint_assessment')}",
            f"DB_USER={self.db_config.get('postgresql', {}).get('username', 'postgres')}",
            "",
            "echo 'Starting ETL Pipeline SQL execution...'",
            "",
            "# Execute DDL statements first",
            "echo 'Creating tables...'",
        ]
        
        for name in dataset_names:
            script_content.append(f"psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -f {name}_ddl.sql")
        
        script_content.extend([
            "",
            "# Execute INSERT statements",
            "echo 'Inserting data...'",
        ])
        
        for name in dataset_names:
            script_content.append(f"psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -f {name}_insert.sql")
        
        script_content.extend([
            "",
            "echo 'ETL Pipeline execution completed!'",
        ])
        
        script_file = Path(self.sql_output_dir) / "execute_etl.sh"
        with open(script_file, 'w', encoding='utf-8') as f:
            f.write("\\n".join(script_content))
        
        # Make script executable
        script_file.chmod(0o755)
        
        logger.info(f"Created execution script: {script_file}")
    
    def run_pipeline(self) -> Dict[str, pd.DataFrame]:
        """Run the complete ETL pipeline."""
        logger.info("Starting ETL Pipeline...")
        
        # Extract
        raw_datasets = self.extract()
        
        if not raw_datasets:
            logger.warning("No data extracted, pipeline cannot continue")
            return {}
        
        # Transform
        transformed_datasets = self.transform(raw_datasets)
        
        # Load (generate SQL)
        self.load(transformed_datasets)
        
        logger.info("ETL Pipeline completed successfully")
        return transformed_datasets


def main():
    """Main function for testing."""
    pipeline = ETLPipeline()
    datasets = pipeline.run_pipeline()
    
    print("\\nETL Pipeline Results:")
    for name, df in datasets.items():
        print(f"  {name}: {df.shape}")


if __name__ == "__main__":
    import sys
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import setup_logging
    setup_logging()
    
    main()