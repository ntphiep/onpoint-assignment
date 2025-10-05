"""
Data processing module for normalizing and transforming crawled data.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ..utils.config import config, ensure_directory

logger = logging.getLogger(__name__)


class DataProcessor:
    """Data processor for normalizing and transforming crawled data."""
    
    def __init__(self, input_dir: str = None, output_dir: str = None):
        """Initialize data processor."""
        self.input_dir = input_dir or str(Path(__file__).parent.parent.parent / "data" / "raw")
        self.output_dir = output_dir or str(Path(__file__).parent.parent.parent / "data" / "processed")
        ensure_directory(self.output_dir)
    
    def load_json_file(self, file_path: str) -> Dict[str, Any]:
        """Load JSON file safely."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return {}
    
    def process_jsonplaceholder_posts(self) -> Optional[pd.DataFrame]:
        """Process JSONPlaceholder posts data."""
        logger.info("Processing JSONPlaceholder posts...")
        
        file_path = Path(self.input_dir) / "jsonplaceholder_posts.json"
        if not file_path.exists():
            logger.warning(f"Posts file not found: {file_path}")
            return None
        
        data = self.load_json_file(file_path)
        if not data or 'data' not in data:
            logger.warning("No valid data found in posts file")
            return None
        
        posts_data = data['data']
        
        # Normalize posts data
        df = pd.DataFrame(posts_data)
        
        # Add metadata
        df['source'] = 'jsonplaceholder'
        df['crawled_at'] = data.get('timestamp', datetime.now().isoformat())
        df['data_type'] = 'posts'
        
        # Clean and validate data
        df['title'] = df['title'].fillna('').astype(str)
        df['body'] = df['body'].fillna('').astype(str)
        df['userId'] = pd.to_numeric(df['userId'], errors='coerce')
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        
        # Add derived fields
        df['title_length'] = df['title'].str.len()
        df['body_length'] = df['body'].str.len()
        df['word_count'] = df['body'].str.split().str.len()
        
        logger.info(f"Processed {len(df)} posts")
        return df
    
    def process_jsonplaceholder_users(self) -> Optional[pd.DataFrame]:
        """Process JSONPlaceholder users data."""
        logger.info("Processing JSONPlaceholder users...")
        
        file_path = Path(self.input_dir) / "jsonplaceholder_users.json"
        if not file_path.exists():
            logger.warning(f"Users file not found: {file_path}")
            return None
        
        data = self.load_json_file(file_path)
        if not data or 'data' not in data:
            logger.warning("No valid data found in users file")
            return None
        
        users_data = data['data']
        
        # Normalize nested data
        normalized_users = []
        for user in users_data:
            normalized_user = {
                'id': user.get('id'),
                'name': user.get('name'),
                'username': user.get('username'),
                'email': user.get('email'),
                'phone': user.get('phone'),
                'website': user.get('website'),
                'company_name': user.get('company', {}).get('name'),
                'company_catchPhrase': user.get('company', {}).get('catchPhrase'),
                'company_bs': user.get('company', {}).get('bs'),
                'address_street': user.get('address', {}).get('street'),
                'address_suite': user.get('address', {}).get('suite'),
                'address_city': user.get('address', {}).get('city'),
                'address_zipcode': user.get('address', {}).get('zipcode'),
                'address_geo_lat': user.get('address', {}).get('geo', {}).get('lat'),
                'address_geo_lng': user.get('address', {}).get('geo', {}).get('lng'),
            }
            normalized_users.append(normalized_user)
        
        df = pd.DataFrame(normalized_users)
        
        # Add metadata
        df['source'] = 'jsonplaceholder'
        df['crawled_at'] = data.get('timestamp', datetime.now().isoformat())
        df['data_type'] = 'users'
        
        # Clean and validate data
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        df['name'] = df['name'].fillna('').astype(str)
        df['username'] = df['username'].fillna('').astype(str)
        df['email'] = df['email'].fillna('').astype(str)
        
        # Add derived fields
        df['has_website'] = df['website'].notna() & (df['website'] != '')
        df['email_domain'] = df['email'].str.split('@').str[-1]
        
        logger.info(f"Processed {len(df)} users")
        return df
    
    def process_jsonplaceholder_comments(self) -> Optional[pd.DataFrame]:
        """Process JSONPlaceholder comments data."""
        logger.info("Processing JSONPlaceholder comments...")
        
        file_path = Path(self.input_dir) / "jsonplaceholder_comments.json"
        if not file_path.exists():
            logger.warning(f"Comments file not found: {file_path}")
            return None
        
        data = self.load_json_file(file_path)
        if not data or 'data' not in data:
            logger.warning("No valid data found in comments file")
            return None
        
        comments_data = data['data']
        
        # Normalize comments data
        df = pd.DataFrame(comments_data)
        
        # Add metadata
        df['source'] = 'jsonplaceholder'
        df['crawled_at'] = data.get('timestamp', datetime.now().isoformat())
        df['data_type'] = 'comments'
        
        # Clean and validate data
        df['postId'] = pd.to_numeric(df['postId'], errors='coerce')
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        df['name'] = df['name'].fillna('').astype(str)
        df['email'] = df['email'].fillna('').astype(str)
        df['body'] = df['body'].fillna('').astype(str)
        
        # Add derived fields
        df['body_length'] = df['body'].str.len()
        df['word_count'] = df['body'].str.split().str.len()
        df['email_domain'] = df['email'].str.split('@').str[-1]
        
        logger.info(f"Processed {len(df)} comments")
        return df
    
    def process_hackernews_stories(self) -> Optional[pd.DataFrame]:
        """Process Hacker News stories data."""
        logger.info("Processing Hacker News stories...")
        
        file_path = Path(self.input_dir) / "hackernews_stories.json"
        if not file_path.exists():
            logger.warning(f"Hacker News file not found: {file_path}")
            return None
        
        data = self.load_json_file(file_path)
        if not data or 'data' not in data:
            logger.warning("No valid data found in Hacker News file")
            return None
        
        stories_data = data['data']
        
        # Normalize stories data
        df = pd.DataFrame(stories_data)
        
        # Add metadata
        df['source'] = 'hackernews'
        df['crawled_at'] = data.get('scraped_at', datetime.now().isoformat())
        df['data_type'] = 'stories'
        
        # Clean and validate data
        df['title'] = df['title'].fillna('').astype(str)
        df['author'] = df['author'].fillna('').astype(str)
        df['url'] = df['url'].fillna('').astype(str)
        
        # Extract numeric values from score and comments
        df['score_numeric'] = df['score'].str.extract(r'(\\d+)').astype(float)
        df['comments_numeric'] = pd.to_numeric(df['comments_count'], errors='coerce')
        
        # Add derived fields
        df['title_length'] = df['title'].str.len()
        df['has_external_url'] = ~df['url'].str.startswith('item?id=')
        df['domain'] = df['url'].str.extract(r'https?://(?:www\\.)?([^/]+)')
        
        logger.info(f"Processed {len(df)} Hacker News stories")
        return df
    
    def process_reddit_posts(self) -> Optional[pd.DataFrame]:
        """Process Reddit posts data."""
        logger.info("Processing Reddit posts...")
        
        file_path = Path(self.input_dir) / "reddit_python_posts.json"
        if not file_path.exists():
            logger.warning(f"Reddit file not found: {file_path}")
            return None
        
        data = self.load_json_file(file_path)
        if not data or 'data' not in data:
            logger.warning("No valid data found in Reddit file")
            return None
        
        posts_data = data['data']
        
        # Normalize posts data
        df = pd.DataFrame(posts_data)
        
        # Add metadata
        df['source'] = 'reddit'
        df['crawled_at'] = data.get('scraped_at', datetime.now().isoformat())
        df['data_type'] = 'posts'
        
        # Clean and validate data
        df['title'] = df['title'].fillna('').astype(str)
        df['author'] = df['author'].fillna('').astype(str)
        df['selftext'] = df['selftext'].fillna('').astype(str)
        df['score'] = pd.to_numeric(df['score'], errors='coerce')
        df['num_comments'] = pd.to_numeric(df['num_comments'], errors='coerce')
        
        # Convert timestamp
        df['created_datetime'] = pd.to_datetime(df['created_utc'], unit='s', errors='coerce')
        
        # Add derived fields
        df['title_length'] = df['title'].str.len()
        df['selftext_length'] = df['selftext'].str.len()
        df['has_selftext'] = df['selftext_length'] > 0
        df['domain'] = df['url'].str.extract(r'https?://(?:www\\.)?([^/]+)')
        
        logger.info(f"Processed {len(df)} Reddit posts")
        return df
    
    def process_all_data(self) -> Dict[str, pd.DataFrame]:
        """Process all available data sources."""
        logger.info("Processing all data sources...")
        
        datasets = {}
        
        # Process JSONPlaceholder data
        posts_df = self.process_jsonplaceholder_posts()
        if posts_df is not None:
            datasets['posts'] = posts_df
        
        users_df = self.process_jsonplaceholder_users()
        if users_df is not None:
            datasets['users'] = users_df
        
        comments_df = self.process_jsonplaceholder_comments()
        if comments_df is not None:
            datasets['comments'] = comments_df
        
        # Process web scraped data
        hackernews_df = self.process_hackernews_stories()
        if hackernews_df is not None:
            datasets['hackernews_stories'] = hackernews_df
        
        reddit_df = self.process_reddit_posts()
        if reddit_df is not None:
            datasets['reddit_posts'] = reddit_df
        
        # Save processed data
        for name, df in datasets.items():
            output_file = Path(self.output_dir) / f"{name}_processed.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved processed {name} data to {output_file}")
        
        return datasets


def main():
    """Main function for testing."""
    processor = DataProcessor()
    datasets = processor.process_all_data()
    
    for name, df in datasets.items():
        print(f"\\n{name.upper()} Dataset:")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(df.head())


if __name__ == "__main__":
    import sys
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import setup_logging
    setup_logging()
    
    main()