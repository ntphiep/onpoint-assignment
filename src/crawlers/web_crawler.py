# DummyJSON Web Crawler# DummyJSON Web Crawler

import jsonimport json

import requestsimport requests

from datetime import datetimefrom datetime import datetime

from typing import Dict, List, Anyfrom typing import Dict, List, Any

import timeimport time



class WebCrawler:class WebCrawler:

    def __init__(self):    def __init__(self):

        self.session = requests.Session()        self.session = requests.Session()

        self.session.headers.update({        self.session.headers.update({

            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

        })        })

        self.base_url = 'https://dummyjson.com/products'        self.base_url = "https://dummyjson.com/products"

        

    def scrape_ecommerce_products(self, max_products: int = 100):    def scrape_ecommerce_products(self, max_products: int = 100):

        products = []        products = []

        products_per_page = 30        products_per_page = 30

        pages_needed = min((max_products + products_per_page - 1) // products_per_page, 7)        pages_needed = min((max_products + products_per_page - 1) // products_per_page, 7)

                

        print(f'Scraping up to {max_products} products from DummyJSON API...')        print(f"Scraping up to {max_products} products from DummyJSON API...")

                

        for page in range(pages_needed):        for page in range(pages_needed):

            skip = page * products_per_page            skip = page * products_per_page

            limit = min(products_per_page, max_products - len(products))            limit = min(products_per_page, max_products - len(products))

                        

            if limit <= 0:            if limit <= 0:

                break                break

                        

            url = f'{self.base_url}?limit={limit}&skip={skip}'            url = f"{self.base_url}?limit={limit}&skip={skip}"

            print(f'Fetching page {page + 1}: {url}')            print(f"Fetching page {page + 1}: {url}")

                        

            try:            try:

                response = self.session.get(url, timeout=30)                response = self.session.get(url, timeout=30)

                response.raise_for_status()                response.raise_for_status()

                data = response.json()                data = response.json()

                page_products = data.get('products', [])                page_products = data.get("products", [])

                                

                for product in page_products:                for product in page_products:

                    product_data = {                    product_data = {

                        'id': f'dj_{product.get("id", "unknown")}_{int(datetime.now().timestamp())}',                        "id": f"dj_{product.get(\"id\", \"unknown\")}_{int(datetime.now().timestamp())}",

                        'title': product.get('title', 'Unknown Product'),                        "title": product.get("title", "Unknown Product"),

                        'price': f'${product.get("price", 0):.2f}',                        "price": f"${product.get(\"price\", 0):.2f}",

                        'description': product.get('description', 'No description available'),                        "description": product.get("description", "No description available"),

                        'rating': round(product.get('rating', 0), 2),                        "rating": round(product.get("rating", 0), 2),

                        'reviews_count': len(product.get('reviews', [])),                        "reviews_count": len(product.get("reviews", [])),

                        'product_url': f'https://dummyjson.com/products/{product.get("id", "")}',                        "product_url": f"https://dummyjson.com/products/{product.get(\"id\", \"\")}",

                        'image_url': product.get('thumbnail', ''),                        "image_url": product.get("thumbnail", ""),

                        'category': product.get('category', 'Uncategorized').title(),                        "category": product.get("category", "Uncategorized").title(),

                        'brand': product.get('brand', 'Unknown Brand'),                        "brand": product.get("brand", "Unknown Brand"),

                        'stock': product.get('stock', 0),                        "stock": product.get("stock", 0),

                        'discount_percentage': product.get('discountPercentage', 0),                        "discount_percentage": product.get("discountPercentage", 0),

                        'sku': product.get('sku', ''),                        "sku": product.get("sku", ""),

                        'weight': product.get('weight', 0),                        "weight": product.get("weight", 0),

                        'availability_status': product.get('availabilityStatus', 'Unknown'),                        "availability_status": product.get("availabilityStatus", "Unknown"),

                        'warranty_info': product.get('warrantyInformation', 'No warranty'),                        "warranty_info": product.get("warrantyInformation", "No warranty"),

                        'shipping_info': product.get('shippingInformation', 'No shipping info'),                        "shipping_info": product.get("shippingInformation", "No shipping info"),

                        'return_policy': product.get('returnPolicy', 'No return policy'),                        "return_policy": product.get("returnPolicy", "No return policy"),

                        'tags': ', '.join(product.get('tags', [])),                        "tags": ", ".join(product.get("tags", [])),

                        'marketplace': 'DummyJSON E-commerce',                        "marketplace": "DummyJSON E-commerce",

                        'scraped_at': datetime.now().isoformat(),                        "scraped_at": datetime.now().isoformat(),

                        'source': 'dummyjson_ecommerce_api',                        "source": "dummyjson_ecommerce_api",

                        'meta_created_at': product.get('meta', {}).get('createdAt', ''),                        "meta_created_at": product.get("meta", {}).get("createdAt", ""),

                        'meta_updated_at': product.get('meta', {}).get('updatedAt', '')                        "meta_updated_at": product.get("meta", {}).get("updatedAt", "")

                    }                    }

                    products.append(product_data)                    products.append(product_data)

                                

                if page < pages_needed - 1:                if page < pages_needed - 1:

                    time.sleep(0.5)                    time.sleep(0.5)

                                        

            except Exception as e:            except Exception as e:

                print(f'Error fetching page {page + 1}: {e}')                print(f"Error fetching page {page + 1}: {e}")

                                

        print(f'Successfully scraped {len(products)} products from DummyJSON')        print(f"Successfully scraped {len(products)} products from DummyJSON")

        return products        return products



if __name__ == '__main__':if __name__ == "__main__":

    crawler = WebCrawler()    crawler = WebCrawler()

    products = crawler.scrape_ecommerce_products(max_products=50)    products = crawler.scrape_ecommerce_products(max_products=50)

    if products:    if products:

        print(f'\nSuccessfully scraped {len(products)} products!')        print(f"\nSuccessfully scraped {len(products)} products!")

        print('\nSample product:')        print("\nSample product:")

        sample = products[0]        sample = products[0]

        for key, value in sample.items():        for key, value in sample.items():

            print(f'  {key}: {value}')            print(f"  {key}: {value}")

