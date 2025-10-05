"""
File format converter for exporting data to various formats.
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from ..utils.config import config, ensure_directory

logger = logging.getLogger(__name__)


class FileConverter:
    """Convert processed data to various file formats."""
    
    def __init__(self, input_dir: str = None, output_dir: str = None):
        """Initialize file converter."""
        self.input_dir = input_dir or str(Path(__file__).parent.parent.parent / "data" / "processed")
        self.output_dir = output_dir or str(Path(__file__).parent.parent.parent / "data" / "outputs")
        ensure_directory(self.output_dir)
        
        # Get format configurations
        self.format_config = config.get_file_formats_config()
    
    def load_processed_data(self) -> Dict[str, pd.DataFrame]:
        """Load all processed CSV files."""
        datasets = {}
        input_path = Path(self.input_dir)
        
        for csv_file in input_path.glob("*_processed.csv"):
            dataset_name = csv_file.stem.replace('_processed', '')
            try:
                df = pd.read_csv(csv_file)
                datasets[dataset_name] = df
                logger.info(f"Loaded {dataset_name}: {df.shape}")
            except Exception as e:
                logger.error(f"Error loading {csv_file}: {e}")
        
        return datasets
    
    def export_to_csv(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """Export datasets to CSV format."""
        logger.info("Exporting to CSV format...")
        
        csv_config = self.format_config.get('csv', {})
        encoding = csv_config.get('encoding', 'utf-8')
        separator = csv_config.get('separator', ',')
        
        csv_dir = Path(self.output_dir) / "csv"
        ensure_directory(str(csv_dir))
        
        for name, df in datasets.items():
            try:
                output_file = csv_dir / f"{name}.csv"
                df.to_csv(
                    output_file,
                    index=False,
                    encoding=encoding,
                    sep=separator
                )
                logger.info(f"Exported {name} to CSV: {output_file}")
            except Exception as e:
                logger.error(f"Error exporting {name} to CSV: {e}")
    
    def export_to_parquet(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """Export datasets to Parquet format."""
        logger.info("Exporting to Parquet format...")
        
        parquet_config = self.format_config.get('parquet', {})
        compression = parquet_config.get('compression', 'snappy')
        engine = parquet_config.get('engine', 'pyarrow')
        
        parquet_dir = Path(self.output_dir) / "parquet"
        ensure_directory(str(parquet_dir))
        
        for name, df in datasets.items():
            try:
                output_file = parquet_dir / f"{name}.parquet"
                df.to_parquet(
                    output_file,
                    index=False,
                    compression=compression,
                    engine=engine
                )
                logger.info(f"Exported {name} to Parquet: {output_file}")
            except Exception as e:
                logger.error(f"Error exporting {name} to Parquet: {e}")
    
    def export_to_excel(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """Export datasets to Excel format."""
        logger.info("Exporting to Excel format...")
        
        excel_config = self.format_config.get('excel', {})
        engine = excel_config.get('engine', 'openpyxl')
        
        excel_dir = Path(self.output_dir) / "excel"
        ensure_directory(str(excel_dir))
        
        # Option 1: Individual Excel files for each dataset
        for name, df in datasets.items():
            try:
                output_file = excel_dir / f"{name}.xlsx"
                df.to_excel(
                    output_file,
                    index=False,
                    engine=engine
                )
                logger.info(f"Exported {name} to Excel: {output_file}")
            except Exception as e:
                logger.error(f"Error exporting {name} to Excel: {e}")
        
        # Option 2: Combined workbook with multiple sheets
        try:
            combined_file = excel_dir / "all_datasets.xlsx"
            with pd.ExcelWriter(combined_file, engine=engine) as writer:
                for name, df in datasets.items():
                    # Excel sheet names have a 31 character limit
                    sheet_name = name[:31] if len(name) > 31 else name
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"Exported combined workbook: {combined_file}")
        except Exception as e:
            logger.error(f"Error creating combined Excel workbook: {e}")
    
    def export_summary_report(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """Create a summary report of all datasets."""
        logger.info("Creating summary report...")
        
        summary_data = []
        for name, df in datasets.items():
            summary_info = {
                'dataset': name,
                'rows': len(df),
                'columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
                'data_types': df.dtypes.value_counts().to_dict(),
                'null_values': df.isnull().sum().sum(),
                'columns_list': list(df.columns)
            }
            summary_data.append(summary_info)
        
        # Create summary DataFrame
        summary_df = pd.DataFrame([
            {
                'dataset': item['dataset'],
                'rows': item['rows'],
                'columns': item['columns'],
                'memory_usage_mb': round(item['memory_usage_mb'], 2),
                'null_values': item['null_values'],
                'data_types_count': len(item['data_types'])
            }
            for item in summary_data
        ])
        
        # Save summary
        summary_file = Path(self.output_dir) / "dataset_summary.csv"
        summary_df.to_csv(summary_file, index=False)
        
        # Create detailed summary
        detailed_summary = []
        for item in summary_data:
            detailed_summary.append(f"\\n=== {item['dataset'].upper()} ===")
            detailed_summary.append(f"Rows: {item['rows']:,}")
            detailed_summary.append(f"Columns: {item['columns']}")
            detailed_summary.append(f"Memory Usage: {item['memory_usage_mb']:.2f} MB")
            detailed_summary.append(f"Null Values: {item['null_values']:,}")
            detailed_summary.append(f"Data Types: {item['data_types']}")
            detailed_summary.append(f"Columns: {', '.join(item['columns_list'])}")
        
        summary_text_file = Path(self.output_dir) / "dataset_summary.txt"
        with open(summary_text_file, 'w', encoding='utf-8') as f:
            f.write("\\n".join(detailed_summary))
        
        logger.info(f"Created summary reports: {summary_file}, {summary_text_file}")
    
    def get_file_sizes(self) -> Dict[str, Dict[str, float]]:
        """Get file sizes for different formats."""
        file_sizes = {}
        output_path = Path(self.output_dir)
        
        formats = ['csv', 'parquet', 'excel']
        for format_name in formats:
            format_dir = output_path / format_name
            if format_dir.exists():
                format_sizes = {}
                for file_path in format_dir.iterdir():
                    if file_path.is_file():
                        size_mb = file_path.stat().st_size / 1024 / 1024
                        format_sizes[file_path.name] = round(size_mb, 3)
                file_sizes[format_name] = format_sizes
        
        return file_sizes
    
    def create_format_comparison(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """Create a comparison of file formats."""
        logger.info("Creating format comparison...")
        
        # Export all formats first
        self.export_to_csv(datasets)
        self.export_to_parquet(datasets)
        self.export_to_excel(datasets)
        
        # Get file sizes
        file_sizes = self.get_file_sizes()
        
        # Create comparison report
        comparison_data = []
        for format_name, files in file_sizes.items():
            for filename, size_mb in files.items():
                comparison_data.append({
                    'format': format_name,
                    'filename': filename,
                    'size_mb': size_mb
                })
        
        if comparison_data:
            comparison_df = pd.DataFrame(comparison_data)
            comparison_file = Path(self.output_dir) / "format_comparison.csv"
            comparison_df.to_csv(comparison_file, index=False)
            
            # Create summary by format
            format_summary = comparison_df.groupby('format')['size_mb'].agg(['count', 'sum', 'mean']).round(3)
            summary_file = Path(self.output_dir) / "format_summary.csv"
            format_summary.to_csv(summary_file)
            
            logger.info(f"Created format comparison: {comparison_file}")
            logger.info(f"Created format summary: {summary_file}")
    
    def run(self) -> None:
        """Run the complete file conversion process."""
        logger.info("Starting file conversion process...")
        
        # Load processed data
        datasets = self.load_processed_data()
        
        if not datasets:
            logger.warning("No processed datasets found")
            return
        
        # Export to all formats
        self.export_to_csv(datasets)
        self.export_to_parquet(datasets)
        self.export_to_excel(datasets)
        
        # Create reports
        self.export_summary_report(datasets)
        self.create_format_comparison(datasets)
        
        logger.info("File conversion process completed")


def main():
    """Main function for testing."""
    converter = FileConverter()
    converter.run()


if __name__ == "__main__":
    import sys
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import setup_logging
    setup_logging()
    
    main()