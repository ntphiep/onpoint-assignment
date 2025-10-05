import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict
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
    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or str(Path(__file__).parent.parent.parent / "data" / "raw")
        ensure_directory(self.output_dir)
        # synchronous crawler, no session required
        
    def crawl_jsonplaceholder(self) -> None:
        """Synchronous crawl of JSONPlaceholder API's users endpoint only."""
        logger.info("Starting JSONPlaceholder API crawling (sync)...")

        api_config = config.get('data_sources.api.jsonplaceholder')
        base_url = api_config['base_url']
        endpoints = api_config['endpoints']

        # Only crawl the 'users' endpoint for now
        if 'users' not in endpoints:
            logger.warning("'users' endpoint not configured for jsonplaceholder; nothing to crawl.")
            return

        try:
            url = f"{base_url}{endpoints['users']}"
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
            output_file = Path(self.output_dir) / f"jsonplaceholder_users.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved users data to {output_file}")
            time.sleep(0.5)  # Be respectful to the API

        except Exception as e:
            logger.error(f"Error crawling users: {e}")
    
    def run(self) -> None:
        """Run the synchronous crawler (currently only users)."""
        self.crawl_jsonplaceholder()


if __name__ == "__main__":
    import sys
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import setup_logging
    setup_logging()
    
    # Run synchronous version for simplicity
    crawler = APICrawler()
    crawler.run()