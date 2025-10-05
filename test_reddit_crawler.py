#!/usr/bin/env python3
"""
Test script for Reddit web crawler
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
    """Setup logging for the test."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def test_reddit_crawler():
    """Test the Reddit web crawler with today's posts logic."""
    print("🔍 Testing Reddit Web Crawler - Latest 50 Posts from Today")
    print("=" * 60)
    
    try:
        # Create crawler instance with target of 50 posts
        crawler = WebCrawler(target_posts=50)
        
        # Run the crawler
        print("📡 Starting Reddit crawling for latest 50 posts from today...")
        start_time = time.time()
        crawler.run()
        end_time = time.time()
        
        # Check if output file was created
        output_file = Path("data/raw/reddit_python_posts.json")
        if output_file.exists():
            import json
            with open(output_file, 'r') as f:
                data = json.load(f)
            
            total_posts = data.get('total_posts', 0)
            target_posts = data.get('target_posts', 50)
            date_filter = data.get('date_filter', 'none')
            
            print(f"✅ Success! Crawled {total_posts}/{target_posts} Reddit posts")
            print(f"📁 Output saved to: {output_file}")
            print(f"⏱️  Processing time: {end_time - start_time:.2f} seconds")
            print(f"🗓️  Date filter: {date_filter}")
            
            # Show post statistics
            if data.get('data'):
                posts = data['data']
                
                # Count posts by time
                today_count = 0
                recent_count = 0
                now = datetime.now()
                
                for post in posts:
                    created_utc = post.get('created_utc', 0)
                    if created_utc:
                        post_time = datetime.fromtimestamp(created_utc)
                        if post_time.date() == now.date():
                            today_count += 1
                        if (now - post_time).total_seconds() < 86400:  # Last 24 hours
                            recent_count += 1
                
                print(f"📊 Statistics:")
                print(f"   • Posts from today: {today_count}")
                print(f"   • Posts from last 24h: {recent_count}")
                print(f"   • Average score: {sum(p.get('score', 0) for p in posts) / len(posts):.1f}")
                
                # Show first few posts
                print(f"\\n📋 Sample posts (showing first 5):")
                for i, post in enumerate(posts[:5]):
                    created_utc = post.get('created_utc', 0)
                    if created_utc:
                        post_time = datetime.fromtimestamp(created_utc)
                        time_str = post_time.strftime("%H:%M")
                    else:
                        time_str = "Unknown"
                    
                    print(f"  {i+1}. [{time_str}] {post.get('title', 'No title')[:50]}...")
                    print(f"     👤 u/{post.get('author', 'Unknown')} | ⬆️ {post.get('score', 0)} | 💬 {post.get('num_comments', 0)}")
                    print()
            
        else:
            print("❌ No output file created - check logs for errors")
    
    except Exception as e:
        print(f"❌ Error running crawler: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function."""
    import time
    setup_logging()
    test_reddit_crawler()

if __name__ == "__main__":
    main()