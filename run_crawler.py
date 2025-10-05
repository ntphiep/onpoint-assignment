#!/usr/bin/env python3
"""
Simple runner script to test the enhanced Reddit crawler
"""
import sys
import logging
import time
from datetime import datetime
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Import the web crawler
from src.crawlers.web_crawler import WebCrawler

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

def run_crawler():
    print("🚀 Running Enhanced Reddit Crawler")
    print("=" * 50)
    print("Target: Latest 50 posts from r/python (today only)")
    print()
    
    # Create crawler with target of 50 posts
    crawler = WebCrawler(target_posts=50)
    
    # Run the crawler
    print("📡 Starting crawl...")
    start_time = time.time()
    
    try:
        crawler.run()
        end_time = time.time()
        
        print(f"✅ Crawl completed in {end_time - start_time:.2f} seconds")
        print()
        
        # Check results
        output_file = Path("data/raw/reddit_python_posts.json")
        if output_file.exists():
            import json
            with open(output_file, 'r') as f:
                data = json.load(f)
            
            print(f"📊 Results:")
            print(f"   • Total posts found: {data.get('total_posts', 0)}")
            print(f"   • Target posts: {data.get('target_posts', 50)}")
            print(f"   • Date filter: {data.get('date_filter', 'none')}")
            print(f"   • Source method: {data.get('method', 'json_api')}")
            print()
            
            if data.get('data'):
                posts = data['data']
                print("📋 Latest posts preview:")
                for i, post in enumerate(posts[:3]):
                    created_utc = post.get('created_utc', 0)
                    if created_utc:
                        post_time = datetime.fromtimestamp(created_utc)
                        time_str = post_time.strftime("%H:%M")
                    else:
                        time_str = "Unknown"
                    
                    print(f"   {i+1}. [{time_str}] {post.get('title', '')[:50]}...")
                    print(f"      👤 u/{post.get('author', 'Unknown')} | ⬆️ {post.get('score', 0)}")
                
                if len(posts) > 3:
                    print(f"   ... and {len(posts) - 3} more posts")
        
        else:
            print("❌ No output file created")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    setup_logging()
    run_crawler()