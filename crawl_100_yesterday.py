#!/usr/bin/env python3

import sys
import json
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "src"))

from crawlers.web_crawler import WebCrawler

def crawl_yesterday_posts():
    print("Crawling 100 posts from yesterday (2025-01-04)...")
    
    crawler = WebCrawler()
    
    target_date = "2025-01-04"
    max_posts = 100
    
    print(f"Target: {max_posts} posts from {target_date}")
    print("Starting crawl...")
    
    posts = crawler.crawl_posts_by_date(target_date, max_posts)
    
    print(f"\nCrawl completed!")
    print(f"Found {len(posts)} posts from {target_date}")
    
    if posts:
        print(f"\nPost statistics:")
        total_score = sum(post.get('data', {}).get('score', 0) for post in posts)
        avg_score = total_score / len(posts) if posts else 0
        
        print(f"- Total posts: {len(posts)}")
        print(f"- Average score: {avg_score:.1f}")
        print(f"- Total combined score: {total_score}")
        
        print(f"\nTop 5 posts by score:")
        sorted_posts = sorted(posts, key=lambda x: x.get('data', {}).get('score', 0), reverse=True)
        
        for i, post in enumerate(sorted_posts[:5], 1):
            post_data = post.get('data', {})
            title = post_data.get('title', 'N/A')
            score = post_data.get('score', 0)
            author = post_data.get('author', 'N/A')
            print(f"  {i}. [{score} pts] {title[:60]}... (by u/{author})")
        
        output_file = Path("data/reddit_yesterday_100_posts.json")
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'target_date': target_date,
                    'requested_posts': max_posts,
                    'actual_posts': len(posts),
                    'total_score': total_score,
                    'average_score': avg_score,
                    'crawl_timestamp': str(datetime.now())
                },
                'posts': posts
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\nData saved to: {output_file}")
        
        csv_file = Path("data/reddit_yesterday_100_posts.csv")
        post_rows = []
        for post in posts:
            post_data = post.get('data', {})
            post_rows.append({
                'id': post_data.get('id', ''),
                'title': post_data.get('title', ''),
                'author': post_data.get('author', ''),
                'score': post_data.get('score', 0),
                'num_comments': post_data.get('num_comments', 0),
                'created_utc': post_data.get('created_utc', 0),
                'url': post_data.get('url', ''),
                'subreddit': post_data.get('subreddit', ''),
                'selftext': post_data.get('selftext', '')[:200] + ('...' if len(post_data.get('selftext', '')) > 200 else '')
            })
        
        import pandas as pd
        df = pd.DataFrame(post_rows)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"CSV saved to: {csv_file}")
        
    else:
        print("No posts found for the target date.")
        print("This might be because:")
        print("1. The date is in the future")
        print("2. Reddit's API doesn't have posts for that specific date")
        print("3. Rate limiting or API access issues")

if __name__ == "__main__":
    from datetime import datetime
    crawl_yesterday_posts()