"""
API data crawler for fetching data from public APIs.
"""
import json
import requests
import time
from datetime import datetime
from typing import Dict, List, Any
import os
import sys

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.config import Config

class APICrawler:
    """Crawler for fetching data from public APIs."""
    
    def __init__(self):
        self.base_url = Config.API_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mini-Pipeline-Crawler/1.0',
            'Accept': 'application/json'
        })
        
    def fetch_posts(self) -> List[Dict[str, Any]]:
        """Fetch posts from JSONPlaceholder API."""
        try:
            response = self.session.get(f"{self.base_url}/posts", timeout=Config.TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching posts: {e}")
            return []
    
    def fetch_users(self) -> List[Dict[str, Any]]:
        """Fetch users from JSONPlaceholder API."""
        try:
            response = self.session.get(f"{self.base_url}/users", timeout=Config.TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching users: {e}")
            return []
    
    def fetch_comments(self) -> List[Dict[str, Any]]:
        """Fetch comments from JSONPlaceholder API."""
        try:
            response = self.session.get(f"{self.base_url}/comments", timeout=Config.TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching comments: {e}")
            return []
    
    def save_to_json(self, data: List[Dict[str, Any]], filename: str) -> str:
        """Save data to JSON file."""
        Config.ensure_directories()
        filepath = os.path.join(Config.RAW_DATA_DIR, filename)
        
        # Add metadata
        output_data = {
            "metadata": {
                "source": "api",
                "timestamp": datetime.now().isoformat(),
                "count": len(data),
                "source_url": self.base_url
            },
            "data": data
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(data)} records to {filepath}")
        return filepath
    
    def crawl_all(self) -> Dict[str, str]:
        """Crawl all data sources and save to JSON files."""
        results = {}
        
        print("Starting API crawling...")
        
        # Fetch posts
        posts = self.fetch_posts()
        if posts:
            results['posts'] = self.save_to_json(posts, 'api_posts.json')
        
        # Fetch users
        users = self.fetch_users()
        if users:
            results['users'] = self.save_to_json(users, 'api_users.json')
        
        # Fetch comments
        comments = self.fetch_comments()
        if comments:
            results['comments'] = self.save_to_json(comments, 'api_comments.json')
        
        print(f"API crawling completed. Files saved: {list(results.keys())}")
        return results

def main():
    """Main function to run the API crawler."""
    crawler = APICrawler()
    results = crawler.crawl_all()
    
    print("\nAPI Crawler Results:")
    for data_type, filepath in results.items():
        print(f"- {data_type}: {filepath}")

if __name__ == "__main__":
    main()