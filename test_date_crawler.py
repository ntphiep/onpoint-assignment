#!/usr/bin/env python3

import sys
import json
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "src"))

from crawlers.web_crawler import WebCrawler

def test_date_based_crawling():
    print("Testing Reddit date-based crawling...")
    
    crawler = WebCrawler()
    
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"\n1. Testing crawl_posts_by_date for {yesterday}")
    posts = crawler.crawl_posts_by_date(yesterday, max_posts=5)
    
    print(f"Found {len(posts)} posts from {yesterday}")
    
    if posts:
        print("\nSample post data:")
        sample_post = posts[0]
        post_data = sample_post.get('data', {})
        print(f"- Title: {post_data.get('title', 'N/A')[:80]}...")
        print(f"- Author: {post_data.get('author', 'N/A')}")
        print(f"- Score: {post_data.get('score', 'N/A')}")
        print(f"- Created: {post_data.get('created_utc', 'N/A')}")
        print(f"- URL: {post_data.get('url', 'N/A')}")
    
    print(f"\n2. Testing crawl_date_range for last 3 days")
    three_days_ago = (date.today() - timedelta(days=3)).strftime("%Y-%m-%d")
    today = date.today().strftime("%Y-%m-%d")
    
    date_range_posts = crawler.crawl_date_range(three_days_ago, today, max_posts_per_day=3)
    
    print(f"Found posts for {len(date_range_posts)} dates:")
    for date_str, posts_list in date_range_posts.items():
        print(f"  {date_str}: {len(posts_list)} posts")
    
    output_file = Path("data/reddit_date_test.json")
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'single_date_posts': posts,
            'date_range_posts': date_range_posts,
            'metadata': {
                'target_date': yesterday,
                'date_range': f"{three_days_ago} to {today}",
                'total_posts': sum(len(posts) for posts in date_range_posts.values())
            }
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to: {output_file}")

if __name__ == "__main__":
    test_date_based_crawling()