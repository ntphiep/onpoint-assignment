"""
S3 uploader for uploading processed files to AWS S3.
"""
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from ..utils.config import config

logger = logging.getLogger(__name__)


class S3Uploader:
    """Upload files to AWS S3 or simulate locally."""
    
    def __init__(self, simulate: bool = None):
        """Initialize S3 uploader."""
        self.aws_config = config.get_aws_config()
        
        # Determine if we should simulate or use real S3
        if simulate is None:
            # Auto-detect: simulate if no real AWS credentials or boto3 not available
            simulate = not BOTO3_AVAILABLE or not self._has_valid_credentials()
        
        self.simulate = simulate
        self.s3_client = None
        
        if not self.simulate and BOTO3_AVAILABLE:
            try:
                self.s3_client = boto3.client(
                    's3',
                    region_name=self.aws_config.get('region', 'us-east-1'),
                    aws_access_key_id=self.aws_config.get('access_key'),
                    aws_secret_access_key=self.aws_config.get('secret_key')
                )
                logger.info("S3 client initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize S3 client: {e}, falling back to simulation")
                self.simulate = True
        
        if self.simulate:
            self.simulation_dir = Path(__file__).parent.parent.parent / "data" / "s3_simulation"
            self.simulation_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"S3 operations will be simulated in: {self.simulation_dir}")
    
    def _has_valid_credentials(self) -> bool:
        """Check if valid AWS credentials are available."""
        access_key = self.aws_config.get('access_key')
        secret_key = self.aws_config.get('secret_key')
        
        # Check if credentials look real (not demo values)
        if (access_key and secret_key and 
            not access_key.startswith('DEMO_') and 
            not secret_key.startswith('DEMO_')):
            return True
        
        # Also check environment variables
        return bool(os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY'))
    
    def upload_file(self, local_file_path: str, s3_key: str) -> bool:
        """Upload a single file to S3 or simulate."""
        local_path = Path(local_file_path)
        
        if not local_path.exists():
            logger.error(f"Local file does not exist: {local_file_path}")
            return False
        
        if self.simulate:
            return self._simulate_upload(local_path, s3_key)
        else:
            return self._real_upload(local_path, s3_key)
    
    def _simulate_upload(self, local_path: Path, s3_key: str) -> bool:
        """Simulate S3 upload by copying to local simulation directory."""
        try:
            # Create directory structure in simulation folder
            s3_path = self.simulation_dir / s3_key
            s3_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            import shutil
            shutil.copy2(local_path, s3_path)
            
            # Create metadata file
            metadata = {
                'original_path': str(local_path),
                's3_key': s3_key,
                'bucket': self.aws_config.get('s3_bucket', 'default-bucket'),
                'size_bytes': local_path.stat().st_size,
                'simulated': True
            }
            
            metadata_path = s3_path.with_suffix(s3_path.suffix + '.metadata')
            import json
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Simulated upload: {local_path.name} -> s3://{self.aws_config.get('s3_bucket')}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error simulating upload for {local_path}: {e}")
            return False
    
    def _real_upload(self, local_path: Path, s3_key: str) -> bool:
        """Perform real S3 upload."""
        try:
            bucket_name = self.aws_config.get('s3_bucket')
            if not bucket_name:
                logger.error("S3 bucket name not configured")
                return False
            
            self.s3_client.upload_file(
                str(local_path),
                bucket_name,
                s3_key,
                ExtraArgs={'ServerSideEncryption': 'AES256'}
            )
            
            logger.info(f"Uploaded: {local_path.name} -> s3://{bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"AWS error uploading {local_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}")
            return False
    
    def upload_directory(self, local_dir_path: str, s3_prefix: str = "") -> Dict[str, bool]:
        """Upload entire directory to S3."""
        local_dir = Path(local_dir_path)
        
        if not local_dir.exists():
            logger.error(f"Local directory does not exist: {local_dir_path}")
            return {}
        
        results = {}
        
        for file_path in local_dir.rglob("*"):
            if file_path.is_file():
                # Calculate relative path for S3 key
                relative_path = file_path.relative_to(local_dir)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\\\", "/").lstrip("/")
                
                success = self.upload_file(str(file_path), s3_key)
                results[str(file_path)] = success
        
        successful_uploads = sum(1 for success in results.values() if success)
        logger.info(f"Uploaded {successful_uploads}/{len(results)} files from {local_dir}")
        
        return results
    
    def upload_processed_data(self) -> Dict[str, bool]:
        """Upload all processed data files to S3."""
        logger.info("Starting upload of processed data...")
        
        # Define directories to upload
        base_dir = Path(__file__).parent.parent.parent / "data"
        upload_dirs = {
            "outputs": base_dir / "outputs",
            "processed": base_dir / "processed"
        }
        
        all_results = {}
        
        for dir_name, dir_path in upload_dirs.items():
            if dir_path.exists():
                logger.info(f"Uploading {dir_name} directory...")
                results = self.upload_directory(str(dir_path), f"data/{dir_name}")
                all_results.update(results)
            else:
                logger.warning(f"Directory not found: {dir_path}")
        
        # Upload summary
        successful = sum(1 for success in all_results.values() if success)
        total = len(all_results)
        logger.info(f"Upload summary: {successful}/{total} files uploaded successfully")
        
        return all_results
    
    def list_uploaded_files(self) -> List[str]:
        """List files that have been uploaded (or simulated)."""
        if self.simulate:
            files = []
            if self.simulation_dir.exists():
                for file_path in self.simulation_dir.rglob("*"):
                    if file_path.is_file() and not file_path.name.endswith('.metadata'):
                        rel_path = file_path.relative_to(self.simulation_dir)
                        files.append(str(rel_path).replace("\\\\", "/"))
            return files
        else:
            # List real S3 objects
            try:
                bucket_name = self.aws_config.get('s3_bucket')
                response = self.s3_client.list_objects_v2(Bucket=bucket_name)
                
                if 'Contents' in response:
                    return [obj['Key'] for obj in response['Contents']]
                else:
                    return []
            except Exception as e:
                logger.error(f"Error listing S3 objects: {e}")
                return []
    
    def create_upload_report(self) -> None:
        """Create a report of uploaded files."""
        files = self.list_uploaded_files()
        
        report_path = Path(__file__).parent.parent.parent / "data" / "upload_report.txt"
        
        with open(report_path, 'w') as f:
            f.write(f"S3 Upload Report\\n")
            f.write(f"=================\\n\\n")
            f.write(f"Upload Mode: {'Simulated' if self.simulate else 'Real S3'}\\n")
            f.write(f"Bucket: {self.aws_config.get('s3_bucket', 'N/A')}\\n")
            f.write(f"Total Files: {len(files)}\\n\\n")
            
            if self.simulate:
                f.write(f"Simulation Directory: {self.simulation_dir}\\n\\n")
            
            f.write("Uploaded Files:\\n")
            f.write("-" * 50 + "\\n")
            for file_key in sorted(files):
                f.write(f"  {file_key}\\n")
        
        logger.info(f"Upload report created: {report_path}")


def main():
    """Main function for testing."""
    uploader = S3Uploader()
    results = uploader.upload_processed_data()
    uploader.create_upload_report()
    
    print(f"\\nUpload Results:")
    for file_path, success in results.items():
        status = "✓" if success else "✗"
        print(f"{status} {Path(file_path).name}")


if __name__ == "__main__":
    import sys
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import setup_logging
    setup_logging()
    
    main()