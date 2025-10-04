"""
API crawler for fetching data from public APIs.
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import requests

try:
    from ..utils.config import config, ensure_directory
except ImportError:
    # Fallback for when running as a script
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import config, ensure_directory

logger = logging.getLogger(__name__)


class APICrawler:
    """Crawler for public APIs."""
    
    def __init__(self, output_dir: str = None):
        """Initialize API crawler."""
        self.output_dir = output_dir or str(Path(__file__).parent.parent.parent / "data" / "raw")
        ensure_directory(self.output_dir)
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=config.get('processing.timeout', 30))
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def fetch_url(self, url: str) -> Dict[str, Any]:
        """Fetch data from a single URL."""
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(f"Successfully fetched data from {url}")
                return {
                    'url': url,
                    'status_code': response.status,
                    'data': data,
                    'timestamp': datetime.now().isoformat(),
                    'size': len(str(data))
                }
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return {
                'url': url,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    async def crawl_jsonplaceholder(self) -> None:
        """Crawl JSONPlaceholder API endpoints."""
        logger.info("Starting JSONPlaceholder API crawling...")
        
        api_config = config.get('data_sources.api.jsonplaceholder')
        base_url = api_config['base_url']
        endpoints = api_config['endpoints']
        
        # Create tasks for concurrent fetching
        tasks = []
        for endpoint_name, endpoint_path in endpoints.items():
            url = f"{base_url}{endpoint_path}"
            tasks.append(self.fetch_url(url))
        
        # Execute all requests concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Save results
        for i, result in enumerate(results):
            endpoint_name = list(endpoints.keys())[i]
            
            if isinstance(result, Exception):
                logger.error(f"Exception for {endpoint_name}: {result}")
                continue
            
            # Save raw JSON data
            output_file = Path(self.output_dir) / f"jsonplaceholder_{endpoint_name}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {endpoint_name} data to {output_file}")
    
    def crawl_jsonplaceholder_sync(self) -> None:
        """Synchronous version of JSONPlaceholder crawling."""
        logger.info("Starting JSONPlaceholder API crawling (sync)...")
        
        api_config = config.get('data_sources.api.jsonplaceholder')
        base_url = api_config['base_url']
        endpoints = api_config['endpoints']
        
        for endpoint_name, endpoint_path in endpoints.items():
            try:
                url = f"{base_url}{endpoint_path}"
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                result = {
                    'url': url,
                    'status_code': response.status_code,
                    'data': response.json(),
                    'timestamp': datetime.now().isoformat(),
                    'size': len(response.content)
                }
                
                # Save raw JSON data
                output_file = Path(self.output_dir) / f"jsonplaceholder_{endpoint_name}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Saved {endpoint_name} data to {output_file}")
                time.sleep(0.5)  # Be respectful to the API
                
            except Exception as e:
                logger.error(f"Error crawling {endpoint_name}: {e}")
    
    async def run_async(self) -> None:
        """Run async crawling."""
        await self.crawl_jsonplaceholder()
    
    def run(self) -> None:
        """Run synchronous crawling."""
        self.crawl_jsonplaceholder_sync()


async def main():
    """Main function for testing."""
    async with APICrawler() as crawler:
        await crawler.run_async()


if __name__ == "__main__":
    import sys
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import setup_logging
    setup_logging()
    
    # Run synchronous version for simplicity
    crawler = APICrawler()
    crawler.run()