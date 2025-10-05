#!/usr/bin/env python3
"""
Test script for the web crawler module.
"""
import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import required modules
import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def ensure_directory(path: str) -> None:
    """Ensure directory exists, create if it doesn't."""
    os.makedirs(path, exist_ok=True)


class WebCrawlerTest:
    """Simplified web scraper for testing."""
    
    def __init__(self, output_dir: str = None):
        """Initialize web crawler."""
        self.output_dir = output_dir or str(Path(__file__).parent / "data" / "raw")
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
        """Safely extract text from BeautifulSoup element."""
        if element:
            return element.get_text(strip=True)
        return ""
    
    def extract_href_safe(self, element) -> str:
        """Safely extract href from BeautifulSoup element."""
        if element:
            return element.get('href', '')
        return ""
    
    def test_simple_request(self) -> bool:
        """Test a simple HTTP request to verify connectivity."""
        logger.info("Testing simple HTTP request...")
        try:
            response = self.session.get('https://httpbin.org/get', timeout=10)
            response.raise_for_status()
            logger.info(f"✅ Simple request successful: {response.status_code}")
            return True
        except Exception as e:
            logger.error(f"❌ Simple request failed: {e}")
            return False
    
    def test_hackernews_crawl(self) -> bool:
        """Test crawling Hacker News."""
        logger.info("Testing Hacker News crawling...")
        
        url = "https://news.ycombinator.com"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all story rows
            stories = []
            story_rows = soup.find_all('tr', class_='athing')
            
            logger.info(f"Found {len(story_rows)} story rows")
            
            for i, story_row in enumerate(story_rows[:5]):  # Test with first 5 stories
                try:
                    story_id = story_row.get('id', '')
                    
                    # Extract title and URL
                    title_element = story_row.find('span', class_='titleline')
                    if title_element:
                        title_link = title_element.find('a')
                        title = self.extract_text_safe(title_link)
                        story_url = self.extract_href_safe(title_link)
                    else:
                        title = ""
                        story_url = ""
                    
                    # Find the metadata row (next sibling)
                    meta_row = story_row.find_next_sibling('tr')
                    score = ""
                    author = ""
                    time_posted = ""
                    comments_count = ""
                    
                    if meta_row:
                        # Extract score
                        score_element = meta_row.find('span', class_='score')
                        score = self.extract_text_safe(score_element)
                        
                        # Extract author
                        author_element = meta_row.find('a', class_='hnuser')
                        author = self.extract_text_safe(author_element)
                        
                        # Extract time
                        time_element = meta_row.find('span', class_='age')
                        time_posted = self.extract_text_safe(time_element)
                        
                        # Extract comments count
                        comments_link = meta_row.find('a', string=re.compile(r'comment'))
                        if comments_link:
                            comments_text = self.extract_text_safe(comments_link)
                            # Extract number from text like "42 comments"
                            comments_match = re.search(r'(\\d+)', comments_text)
                            comments_count = comments_match.group(1) if comments_match else "0"
                    
                    story_data = {
                        'id': story_id,
                        'rank': i + 1,
                        'title': title,
                        'url': story_url,
                        'score': score,
                        'author': author,
                        'time_posted': time_posted,
                        'comments_count': comments_count,
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    stories.append(story_data)
                    logger.info(f"Story {i+1}: {title[:50]}...")
                    
                except Exception as e:
                    logger.warning(f"Error processing story {i}: {e}")
                    continue
            
            # Prepare result structure
            result = {
                'source': 'hackernews',
                'url': url,
                'scraped_at': datetime.now().isoformat(),
                'total_stories': len(stories),
                'data': stories
            }
            
            # Save test results
            output_file = Path(self.output_dir) / "hackernews_test.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Successfully scraped {len(stories)} stories from Hacker News")
            logger.info(f"Results saved to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error crawling Hacker News: {e}")
            return False
    
    def test_reddit_crawl(self) -> bool:
        """Test crawling Reddit."""
        logger.info("Testing Reddit JSON API...")
        
        url = "https://www.reddit.com/r/python.json"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            posts = []
            
            if 'data' in data and 'children' in data['data']:
                for post_data in data['data']['children'][:5]:  # Test with first 5 posts
                    post = post_data.get('data', {})
                    
                    post_info = {
                        'id': post.get('id', ''),
                        'title': post.get('title', ''),
                        'author': post.get('author', ''),
                        'score': post.get('score', 0),
                        'num_comments': post.get('num_comments', 0),
                        'created_utc': post.get('created_utc', 0),
                        'url': post.get('url', ''),
                        'selftext': post.get('selftext', '')[:100] + '...' if post.get('selftext', '') else '',
                        'subreddit': post.get('subreddit', ''),
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    posts.append(post_info)
                    logger.info(f"Post: {post_info['title'][:50]}...")
            
            result = {
                'source': 'reddit_python',
                'url': url,
                'scraped_at': datetime.now().isoformat(),
                'total_posts': len(posts),
                'data': posts
            }
            
            # Save test results
            output_file = Path(self.output_dir) / "reddit_test.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Successfully scraped {len(posts)} posts from Reddit")
            logger.info(f"Results saved to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error crawling Reddit: {e}")
            return False
    
    def run_tests(self):
        """Run all web crawler tests."""
        logger.info("🚀 Starting Web Crawler Tests...")
        logger.info("=" * 50)
        
        tests = [
            ("Simple HTTP Request", self.test_simple_request),
            ("Hacker News Crawling", self.test_hackernews_crawl),
            ("Reddit API Crawling", self.test_reddit_crawl),
        ]
        
        results = {}
        for test_name, test_func in tests:
            logger.info(f"\\n🧪 Running: {test_name}")
            logger.info("-" * 30)
            try:
                results[test_name] = test_func()
            except Exception as e:
                logger.error(f"❌ Test '{test_name}' failed with exception: {e}")
                results[test_name] = False
        
        # Print summary
        logger.info("\\n" + "=" * 50)
        logger.info("📊 TEST SUMMARY")
        logger.info("=" * 50)
        
        passed = sum(results.values())
        total = len(results)
        
        for test_name, passed_test in results.items():
            status = "✅ PASS" if passed_test else "❌ FAIL"
            logger.info(f"{status} - {test_name}")
        
        logger.info(f"\\n🎯 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            logger.info("🎉 All tests passed! Web crawler is working correctly.")
        else:
            logger.warning(f"⚠️  {total - passed} test(s) failed. Please check the logs above.")
        
        return results


if __name__ == "__main__":
    # Create and run tests
    crawler = WebCrawlerTest()
    results = crawler.run_tests()