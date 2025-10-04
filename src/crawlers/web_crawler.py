import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

try:
    from ..utils.config import config, ensure_directory
except ImportError:
    # Fallback for when running as a script
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import config, ensure_directory

logger = logging.getLogger(__name__)


class WebCrawler:
    
    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or str(Path(__file__).parent.parent.parent / "data" / "raw")
        ensure_directory(self.output_dir)
        
        # Setup session with headers to appear more like a regular browser
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
    
    def extract_text_safe(self, element) -> str:
        if element:
            return element.get_text(strip=True)
        return ""
    
    def extract_href_safe(self, element) -> str:
        if element:
            return element.get('href', '')
        return ""
    

    
    def crawl_reddit_python(self) -> None:
        logger.info("Starting Reddit Python subreddit crawling...")
        
        # Try multiple Reddit endpoints
        endpoints = [
            "https://www.reddit.com/r/python.json",
            "https://old.reddit.com/r/python.json",
            "https://www.reddit.com/r/python/.json"
        ]
        
        for url in endpoints:
            try:
                logger.info(f"Trying endpoint: {url}")
                
                # Enhanced headers to avoid blocking
                headers = {
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Cache-Control': 'max-age=0'
                }
                
                response = requests.get(url, headers=headers, timeout=30)
                logger.info(f"Response status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    posts = self.process_reddit_data(data, url)
                    if posts:
                        logger.info(f"Successfully scraped {len(posts)} Reddit posts")
                        return
                else:
                    logger.warning(f"Failed to access {url}: Status {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Error with endpoint {url}: {e}")
                continue
        
        # If all endpoints fail, try web scraping fallback
        logger.info("All JSON endpoints failed, trying web scraping...")
        self.crawl_reddit_web_fallback()
    
    def process_reddit_data(self, data, url):
        posts = []
        
        if 'data' in data and 'children' in data['data']:
            for post_data in data['data']['children']:
                post = post_data.get('data', {})
                
                post_info = {
                    'id': post.get('id', ''),
                    'title': post.get('title', ''),
                    'author': post.get('author', ''),
                    'score': post.get('score', 0),
                    'num_comments': post.get('num_comments', 0),
                    'created_utc': post.get('created_utc', 0),
                    'url': post.get('url', ''),
                    'selftext': post.get('selftext', ''),
                    'subreddit': post.get('subreddit', ''),
                    'upvote_ratio': post.get('upvote_ratio', 0),
                    'is_self': post.get('is_self', False),
                    'scraped_at': datetime.now().isoformat()
                }
                
                posts.append(post_info)
        
        if posts:
            result = {
                'source': 'reddit_python',
                'url': url,
                'scraped_at': datetime.now().isoformat(),
                'total_posts': len(posts),
                'data': posts
            }
            
            # Save raw JSON data
            output_file = Path(self.output_dir) / "reddit_python_posts.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(posts)} Reddit posts to {output_file}")
            return posts
        
        return None
    
    def crawl_reddit_web_fallback(self):
        try:
            url = "https://old.reddit.com/r/python"
            logger.info(f"Attempting web scraping fallback for: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            posts = []
            post_elements = soup.find_all('div', class_='thing')[:10]  # Get first 10 posts
            
            for i, post_element in enumerate(post_elements):
                try:
                    title_element = post_element.find('a', class_='title')
                    title = self.extract_text_safe(title_element) if title_element else ""
                    
                    author_element = post_element.find('a', class_='author')
                    author = self.extract_text_safe(author_element) if author_element else ""
                    
                    score_element = post_element.find('div', class_='score')
                    score = self.extract_text_safe(score_element) if score_element else "0"
                    
                    comments_element = post_element.find('a', class_='comments')
                    comments_text = self.extract_text_safe(comments_element) if comments_element else "0"
                    
                    post_data = {
                        'id': post_element.get('data-fullname', f'web_{i}'),
                        'title': title,
                        'author': author,
                        'score': score,
                        'comments': comments_text,
                        'scraped_at': datetime.now().isoformat(),
                        'source_method': 'web_scraping'
                    }
                    
                    posts.append(post_data)
                    
                except Exception as e:
                    logger.warning(f"Error processing post {i}: {e}")
                    continue
            
            if posts:
                result = {
                    'source': 'reddit_python_web',
                    'url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'total_posts': len(posts),
                    'method': 'web_scraping_fallback',
                    'data': posts
                }
                
                output_file = Path(self.output_dir) / "reddit_python_posts.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Saved {len(posts)} Reddit posts via web scraping to {output_file}")
            else:
                logger.warning("No posts extracted via web scraping")
                
        except Exception as e:
            logger.error(f"Web scraping fallback failed: {e}")
            # Create minimal error file
            error_result = {
                'source': 'reddit_python',
                'error': str(e),
                'scraped_at': datetime.now().isoformat(),
                'data': []
            }
            
            output_file = Path(self.output_dir) / "reddit_python_posts.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(error_result, f, indent=2, ensure_ascii=False)
    
    
    
    def run(self) -> None:
        self.crawl_reddit_python()
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


def main():
    crawler = WebCrawler()
    crawler.run()


if __name__ == "__main__":
    import sys
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import setup_logging
    setup_logging()
    
    main()