import os
import sys
import json
import argparse
import logging

logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logging.getLogger("scrapy").setLevel(logging.CRITICAL)
logging.getLogger("twisted").setLevel(logging.CRITICAL)

logging.getLogger("twisted").propagate = False
logging.getLogger("scrapy.core.engine").setLevel(logging.CRITICAL)
logging.getLogger("scrapy.dupefilter").setLevel(logging.CRITICAL)
logging.getLogger("scrapy.downloadermiddlewares").setLevel(logging.CRITICAL)

from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
import multiprocessing
from multiprocessing import Process
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from fetcher.product_fetcher import ProductFetcher

class AshleyURLSpider(scrapy.Spider):
    """Fast parallel URL fetcher from manufacturer API"""
    name = "ashley_url_fetcher"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        base_api_arg = kwargs.get('base_api', '')
        if isinstance(base_api_arg, str):
            self.base_apis = [base_api_arg] if base_api_arg else ['https://colemanfurniture.com/manufacturer/detail/250']
        elif isinstance(base_api_arg, list):
            self.base_apis = base_api_arg
        else:
            self.base_apis = ['https://colemanfurniture.com/manufacturer/detail/250']
            
        self.ashley_urls = set()
        self.start_page = int(kwargs.get('start_page', 1))
        self.end_page = int(kwargs.get('end_page', 150))
        self.url_list = kwargs.get('url_list')
        self.concurrent_pages = int(kwargs.get('concurrent_pages', 20))
        
    async def start(self):
        for req in self.start_requests():
            yield req

    def start_requests(self):
        """Start multiple page requests concurrently for all base APIs"""
        for base_url in self.base_apis:
            for page in range(self.start_page, self.end_page + 1):
                yield self.create_page_request(base_url, page)
    
    def create_page_request(self, base_url, page):
        if '?' in base_url:
            url = f"{base_url}&p={page}"
        else:
            url = f"{base_url}?p={page}"
            
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://colemanfurniture.com/'
        }
        
        # If it is the manufacturer detail API, we use AJAX headers
        if '/manufacturer/detail/' in base_url:
            headers['Accept'] = 'application/json, text/javascript, */*; q=0.01'
            headers['X-Requested-With'] = 'XMLHttpRequest'
            
        return scrapy.Request(
            url,
            callback=self.parse_page,
            meta={'page': page, 'base_url': base_url},
            headers=headers,
            dont_filter=True
        )
    
    def parse_page(self, response):
        page = response.meta['page']
        
        if response.status != 200 or len(response.body) < 50:
            return
        
        page_urls = []
        seen_on_page = set()
        is_json = False
        
        # Try to parse as JSON first
        try:
            content_type = response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore').lower()
            if 'json' in content_type or response.body.strip().startswith(b'{'):
                data = response.json()
                is_json = True
                data_obj = data.get('data', {})
                content = data_obj.get('content', {})
                products = []
                if content:
                    products = content.get('products', [])
                    if isinstance(products, dict):
                        products = [products]
                    
                for product in products:
                    url = product.get('url')
                    if url and isinstance(url, str) and url.strip():
                        url = url.strip().strip('"').strip("'")
                        
                        if not url.startswith(('http://', 'https://')):
                            if url.startswith('/'):
                                full_url = urljoin('https://colemanfurniture.com', url)
                            else:
                                full_url = urljoin('https://colemanfurniture.com/', url)
                        else:
                            full_url = url
                        
                        parsed = urlparse(full_url)
                        normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
                        
                        if parsed.scheme and parsed.netloc:
                            if normalized_url not in seen_on_page:
                                seen_on_page.add(normalized_url)
                                page_urls.append(normalized_url)
                            else:
                                logger.debug(f"Duplicate main URL on same page {page}: {normalized_url}")
                        else:
                            logger.warning(f"Invalid main URL on page {page}: {url}")
                    
                    associated_bundles = product.get('associatedBundles', [])
                    if isinstance(associated_bundles, list):
                        for bundle in associated_bundles:
                            if isinstance(bundle, dict):
                                bundle_url = bundle.get('url')
                                if bundle_url and isinstance(bundle_url, str) and bundle_url.strip():
                                    bundle_url = bundle_url.strip().strip('"').strip("'")
                                    
                                    if not bundle_url.startswith(('http://', 'https://')):
                                        if bundle_url.startswith('/'):
                                            full_bundle_url = urljoin('https://colemanfurniture.com', bundle_url)
                                        else:
                                            full_bundle_url = urljoin('https://colemanfurniture.com/', bundle_url)
                                    else:
                                        full_bundle_url = bundle_url
                                    
                                    parsed = urlparse(full_bundle_url)
                                    normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
                                    
                                    if parsed.scheme and parsed.netloc:
                                        if normalized_url not in seen_on_page:
                                            seen_on_page.add(normalized_url)
                                            page_urls.append(normalized_url)
                                            logger.debug(f"Found associated bundle URL: {normalized_url}")
                                        else:
                                            logger.debug(f"Duplicate bundle URL on same page {page}: {normalized_url}")
                                    else:
                                        logger.warning(f"Invalid bundle URL on page {page}: {bundle_url}")

                # Secondary attempt: if no products found, recursively scan the whole JSON for PDP URLs
                if not page_urls:
                    def add_scanned_url(url_str):
                        url_str = url_str.strip().strip('"').strip("'")
                        if not url_str:
                            return
                        try:
                            if url_str.startswith(('http://', 'https://')):
                                parsed_u = urlparse(url_str)
                                path = parsed_u.path.strip('/')
                                full_url = url_str
                            else:
                                path = url_str.split('?')[0].strip('/')
                                if url_str.startswith('/'):
                                    full_url = urljoin('https://colemanfurniture.com', url_str)
                                else:
                                    full_url = urljoin('https://colemanfurniture.com/', url_str)
                            
                            # Filter PDP URLs (directly under root domain, i.e., no additional slash in path)
                            if path and '/' not in path and path.endswith(('.htm', '.html')):
                                parsed_u = urlparse(full_url)
                                normalized_url = f"{parsed_u.scheme}://{parsed_u.netloc}{parsed_u.path.rstrip('/')}"
                                if parsed_u.scheme and parsed_u.netloc and normalized_url not in seen_on_page:
                                    seen_on_page.add(normalized_url)
                                    page_urls.append(normalized_url)
                        except Exception:
                            pass

                    def scan_json(item):
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if k == 'url' and isinstance(v, str):
                                    add_scanned_url(v)
                                else:
                                    scan_json(v)
                        elif isinstance(item, list):
                            for v in item:
                                scan_json(v)
                        elif isinstance(item, str):
                            if item.startswith(('http://', 'https://', '/')) and '.htm' in item:
                                add_scanned_url(item)

                    scan_json(data)
        except Exception as e:
            logger.debug(f"JSON parsing failed or skipped for page {page}, falling back to HTML parsing: {e}")
            is_json = False

        # Fallback to HTML parsing if not JSON or JSON was empty
        if not is_json or not page_urls:
            try:
                # Find all product links in product-img-frame
                extracted_hrefs = response.css('.product-img-frame a::attr(href)').getall()
                
                # Also fall back to general category product list item links
                if not extracted_hrefs:
                    extracted_hrefs = response.css('.product-item-link::attr(href)').getall()
                if not extracted_hrefs:
                    extracted_hrefs = response.css('a.product-item-photo::attr(href)').getall()
                if not extracted_hrefs:
                    extracted_hrefs = response.css('.product-grid-item a::attr(href)').getall()

                for url in extracted_hrefs:
                    if url and isinstance(url, str) and url.strip():
                        url = url.strip().strip('"').strip("'")
                        
                        if not url.startswith(('http://', 'https://')):
                            if url.startswith('/'):
                                full_url = urljoin('https://colemanfurniture.com', url)
                            else:
                                full_url = urljoin('https://colemanfurniture.com/', url)
                        else:
                            full_url = url
                        
                        parsed = urlparse(full_url)
                        normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
                        
                        if parsed.scheme and parsed.netloc:
                            if normalized_url not in seen_on_page:
                                seen_on_page.add(normalized_url)
                                page_urls.append(normalized_url)
                            else:
                                logger.debug(f"Duplicate URL on same page {page}: {normalized_url}")
            except Exception as e:
                logger.error(f"HTML parsing failed for page {page}: {e}")

        # Add to the unique collection of URLs
        new_urls_count = 0
        for url in page_urls:
            if url not in self.ashley_urls:
                self.ashley_urls.add(url)
                if self.url_list is not None:
                    self.url_list.append(url)
                new_urls_count += 1
        
        logger.info(f"Page {page}: Found {len(page_urls)} valid products ({new_urls_count} new, {len(page_urls) - new_urls_count} already seen in previous pages)")
    
    def closed(self, reason):
        logger.info(f"Collected {len(self.ashley_urls)} Ashley product URLs from pages {self.start_page}-{self.end_page}")

def clean_url_string(url):
    """Clean individual URL string"""
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip().strip('"').strip("'")
    
    if not url:
        return None
    
    if not url.startswith(('http://', 'https://')):
        if url.startswith('/'):
            url = f"https://colemanfurniture.com{url}"
        else:
            url = f"https://colemanfurniture.com/{url}"
    
    return url

def validate_urls_file(file_path):
    """Validate and clean URLs in the input file"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            return []
        
        if isinstance(data, dict) and 'urls' in data:
            urls = data['urls']
            is_dict_format = True
        elif isinstance(data, list):
            urls = data
            is_dict_format = False
            data = {"urls": data, "total_urls": 0, "manufacturer_id": "250"}
        else:
            logger.error(f"Unexpected JSON structure in {file_path}")
            return []
        
        valid_urls = []
        for url in urls:
            cleaned_url = clean_url_string(url)
            if cleaned_url:
                parsed = urlparse(cleaned_url)
                if parsed.scheme and parsed.netloc and '.' in parsed.netloc:
                    valid_urls.append(cleaned_url)
                else:
                    logger.warning(f"Invalid URL after cleaning: '{url}' -> '{cleaned_url}'")
            else:
                logger.warning(f"Failed to clean URL: '{url}'")
        
        if is_dict_format:
            data['urls'] = valid_urls
            data['total_urls'] = len(valid_urls)
        else:
            data['urls'] = valid_urls
            data['total_urls'] = len(valid_urls)
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Validated URLs file: {len(valid_urls)} valid URLs (removed {len(urls) - len(valid_urls)} invalid)")
        
        if valid_urls:
            logger.info(f"Sample cleaned URL: {valid_urls[0]}")
        
        return valid_urls
        
    except Exception as e:
        logger.error(f"Error validating URLs file: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_export_fields(include_row_json=True):
    fields = [
        'Ref Product URL',
        'Ref Product ID', 
        'Ref Variant ID',
        'Ref Category',
        'Ref Category URL',
        'Product Type',
        'Ref Brand Name',
        'Collection Name',
        'Ref Product Name',
        'Ref SKU',
        'Ref MPN',
        'Ref GTIN',
        'Ref Price',
        'Ref Main Image',
        'Ref Quantity',
        'Ref Group Attr 1',
        'Ref Group Attr 2',
        'Ref Images',
        'Ref Dimensions',
        'Ref Status',
        'Ref Highlights'
    ]
    if include_row_json:
        fields.append('Row JSON')
    fields.append('Date Scrapped')
    return fields

def str2bool(v):
    if isinstance(v, bool):
        return v
    if str(v).lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif str(v).lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def run_scraper_chunk(chunk_id, total_chunks, chunk_urls, output_dir, job_id, manufacturer_id, 
                     product_concurrency, sitemap_offset, max_sitemaps, max_urls_per_sitemap,
                     include_row_json=True):
    chunk_output = f'{output_dir}/output_ashley_{manufacturer_id}_{job_id}_chunk_{chunk_id}.csv'
    logger.info(f"Chunk {chunk_id + 1}/{total_chunks}: Starting with {len(chunk_urls)} URLs -> {chunk_output}")
    settings = get_project_settings()
    settings.set('FEED_URI', chunk_output)
    settings.set('FEED_FORMAT', 'csv')
    settings.set('CONCURRENT_REQUESTS', product_concurrency)
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', min(product_concurrency, 12))
    settings.set('DOWNLOAD_DELAY', 0.2)
    settings.set('RANDOMIZE_DOWNLOAD_DELAY', True)
    settings.set('DOWNLOAD_TIMEOUT', 30)
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 1)
    settings.set('RETRY_HTTP_CODES', [405, 429, 500, 502, 503, 504, 400, 403, 404, 408])
    settings.set('COOKIES_ENABLED', True)
    settings.set('ROBOTSTXT_OBEY', False)
    settings.set('LOG_LEVEL', 'INFO')
    settings.set('LOG_STDOUT', True)
    settings.set('FEED_EXPORT_FIELDS', get_export_fields(include_row_json))
    settings.set('DUPEFILTER_CLASS', 'scrapy.dupefilters.RFPDupeFilter')
    
    process = CrawlerProcess(settings)
    process.crawl(ProductFetcher,
                 website_url="https://colemanfurniture.com",
                 ashley_urls=chunk_urls,
                 is_ashley=True,
                 chunk_mode=True,
                 chunk_id=chunk_id,
                 total_chunks=total_chunks,
                 sitemap_offset=sitemap_offset,
                 max_sitemaps=max_sitemaps,
                 max_urls_per_sitemap=max_urls_per_sitemap,
                 job_id=f"{job_id}_chunk_{chunk_id}")
    
    try:
        process.start()
        logger.info(f"Chunk {chunk_id + 1}/{total_chunks}: Completed -> {chunk_output}")
        return chunk_output
    except Exception as e:
        logger.error(f"Chunk {chunk_id + 1}/{total_chunks}: Failed - {e}")
        return None

def split_into_chunks(url_list, chunk_size):
    chunks = []
    for i in range(0, len(url_list), chunk_size):
        chunks.append(url_list[i:i + chunk_size])
    return chunks

def main():
    parser = argparse.ArgumentParser(description='Ashley Furniture Scraper')
    
    parser.add_argument('--base-api', nargs='+', default='https://colemanfurniture.com/manufacturer/detail/250', help='Base API endpoint URL(s) (e.g. https://colemanfurniture.com/manufacturer/detail/250)')
    parser.add_argument('--manufacturer-id', default=None, help='Manufacturer ID (e.g. 250)')
    parser.add_argument('--start-page', type=int, default=1, help='Start page number')
    parser.add_argument('--end-page', type=int, default=150, help='End page number')
    parser.add_argument('--chunk', type=int, default=0, help='Chunk ID for URL collection')
    parser.add_argument('--url-concurrency', type=int, default=20, help='Concurrent URL requests')
    
    parser.add_argument('--urls-file', help='JSON file containing Ashley URLs')
    parser.add_argument('--product-concurrency', type=int, default=12, help='Concurrent product requests per chunk')
    
    parser.add_argument('--product-chunks', type=int, default=1, help='Number of parallel chunks for product scraping')
    parser.add_argument('--chunk-size', type=int, default=0, help='Number of URLs per chunk (0 = auto-calculate)')
    
    parser.add_argument('--job-id', default='ashley', help='Job identifier')
    parser.add_argument('--output-dir', default='output', help='Output directory')
    parser.add_argument('--sitemap-offset', type=int, default=0, help='Sitemap offset (compatibility)')
    parser.add_argument('--max-sitemaps', type=int, default=0, help='Max sitemaps (compatibility)')
    parser.add_argument('--max-urls-per-sitemap', type=int, default=0, help='Max URLs per sitemap (compatibility)')
    parser.add_argument('--include-row-json', type=str2bool, nargs='?', const=True, default=True, help='Include raw Row JSON in product CSV (default: True)')
    parser.add_argument('--no-row-json', dest='include_row_json', action='store_false', help='Exclude raw Row JSON in product CSV')
    
    args = parser.parse_args()
    
    # Extract and parse all base URLs
    base_apis = []
    if isinstance(args.base_api, list):
        for item in args.base_api:
            base_apis.extend([u.strip() for u in item.split() if u.strip()])
    elif isinstance(args.base_api, str):
        base_apis = [u.strip() for u in args.base_api.split() if u.strip()]
        
    if not base_apis:
        base_apis = ['https://colemanfurniture.com/manufacturer/detail/250']

    # Determine manufacturer_id and sync base_api
    if args.manufacturer_id:
        mfg_id = str(args.manufacturer_id).strip()
        if base_apis == ['https://colemanfurniture.com/manufacturer/detail/250'] and mfg_id != '250':
            base_apis = [f'https://colemanfurniture.com/manufacturer/detail/{mfg_id}']
    else:
        # Derive manufacturer_id from the first base_api in the list
        first_base_api = base_apis[0]
        try:
            parsed_url = urlparse(first_base_api)
            mfg_id = parsed_url.path.rstrip('/').split('/')[-1]
            if not mfg_id:
                mfg_id = '250'
        except Exception:
            mfg_id = '250'
    args.manufacturer_id = mfg_id
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    if args.urls_file is None:
        logger.info("="*60) 
        logger.info(f"MODE: Collect Ashley URLs - Chunk {args.chunk}")
        logger.info(f"Pages: {args.start_page} - {args.end_page}")
        logger.info("="*60)
        
        url_list = []
        valid_urls = []
        listing_urls = []

        for url in base_apis:
            parsed_base = urlparse(url)
            if parsed_base.path.endswith('.htm') or parsed_base.path.endswith('.html'):
                logger.info(f"Direct product URL detected — bypassing URL collection spider: {url}")
                valid_urls.append(url)
            else:
                listing_urls.append(url)

        if listing_urls:
            settings = {
                "LOG_LEVEL": "INFO",
                "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "CONCURRENT_REQUESTS": args.url_concurrency,
                "CONCURRENT_REQUESTS_PER_DOMAIN": args.url_concurrency,
                "DOWNLOAD_DELAY": 0.25,
                "COOKIES_ENABLED": False,
                "ROBOTSTXT_OBEY": False,
                "DOWNLOAD_TIMEOUT": 10,
                "RETRY_ENABLED": False,
            }
            
            process = CrawlerProcess(settings)
            process.crawl(AshleyURLSpider,
                          base_api=listing_urls,
                          start_page=args.start_page,
                          end_page=args.end_page,
                          url_list=url_list,
                          concurrent_pages=args.url_concurrency)
            process.start()
            
            for url in url_list:
                cleaned_url = clean_url_string(url)
                if cleaned_url:
                    parsed = urlparse(cleaned_url)
                    if parsed.scheme and parsed.netloc and '.' in parsed.netloc:
                        valid_urls.append(cleaned_url)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'{args.output_dir}/ashley_urls_chunk_{args.chunk}_{args.job_id}_{timestamp}.json'
        
        with open(output_file, 'w') as f:
            json.dump({
                "manufacturer_id": args.manufacturer_id,
                "chunk": args.chunk,
                "start_page": args.start_page,
                "end_page": args.end_page,
                "total_urls": len(valid_urls),
                "urls": valid_urls
            }, f, indent=2)
        
        logger.info(f"Saved {len(valid_urls)} valid URLs to {output_file}")
        
        if valid_urls:
            logger.info(f"Sample URL: {valid_urls[0]}")
        
        print(f"OUTPUT_FILE={output_file}")
    
    else:
        logger.info("="*60)
        logger.info(f"MODE: Scrape Ashley Products - PARALLEL CHUNKS")
        logger.info(f"URLs file: {args.urls_file}")
        logger.info(f"Parallel chunks: {args.product_chunks}")
        logger.info(f"URLs per chunk: {args.chunk_size if args.chunk_size > 0 else 'auto'}")
        logger.info(f"Concurrency per chunk: {args.product_concurrency}")
        logger.info("="*60)
        
        if not os.path.exists(args.urls_file):
            logger.error(f"URLs file not found: {args.urls_file}")
            sys.exit(1)
        
        all_ashley_urls = validate_urls_file(args.urls_file)
        
        if not all_ashley_urls:
            logger.error("No valid URLs found to scrape!")
            sys.exit(1)
        
        total_urls = len(all_ashley_urls)
        logger.info(f"Total URLs to scrape: {total_urls}")
        
        if args.product_chunks <= 1:
            logger.info("Running in single process mode...")
            
            timestamp = os.getenv('GITHUB_RUN_ID', 'local')
            domain = f"ashley_{args.manufacturer_id}"
            output_file = f'{args.output_dir}/output_{domain}_{args.job_id}_{timestamp}.csv'
            
            settings = get_project_settings()
            
            settings.set('FEED_URI', output_file)
            settings.set('FEED_FORMAT', 'csv')
            settings.set('CONCURRENT_REQUESTS', args.product_concurrency)
            settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', min(args.product_concurrency, 12))
            settings.set('DOWNLOAD_DELAY', 0.2)
            settings.set('RANDOMIZE_DOWNLOAD_DELAY', True)
            settings.set('DOWNLOAD_TIMEOUT', 30)
            settings.set('RETRY_ENABLED', True)
            settings.set('RETRY_TIMES', 1)
            settings.set('COOKIES_ENABLED', True)
            settings.set('ROBOTSTXT_OBEY', False)
            settings.set('FEED_EXPORT_FIELDS', get_export_fields(args.include_row_json))
            settings.set('DUPEFILTER_CLASS', 'scrapy.dupefilters.RFPDupeFilter')
            
            process = CrawlerProcess(settings)
            process.crawl(ProductFetcher,
                         website_url="https://colemanfurniture.com",
                         ashley_urls=all_ashley_urls,
                         is_ashley=True,
                         chunk_mode=False,
                         sitemap_offset=args.sitemap_offset,
                         max_sitemaps=args.max_sitemaps,
                         max_urls_per_sitemap=args.max_urls_per_sitemap,
                         job_id=args.job_id)
            process.start()
            
            logger.info(f"Scraped {len(all_ashley_urls)} Ashley products to {output_file}")
            print(f"OUTPUT_FILE={output_file}")
            
        else:
            if args.chunk_size > 0:
                chunk_size = args.chunk_size
            else:
                chunk_size = total_urls // args.product_chunks
                if total_urls % args.product_chunks != 0:
                    chunk_size += 1
            
            chunks = split_into_chunks(all_ashley_urls, chunk_size)
            logger.info(f"Split into {len(chunks)} chunks of ~{chunk_size} URLs each")
            
            chunks = chunks[:args.product_chunks]
            logger.info(f"Processing {len(chunks)} chunks in parallel")
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_timestamp = f"{args.job_id}_{timestamp}"
            
            processes = []
            chunk_outputs = []
            
            start_time = time.time()
            
            for i, chunk_urls in enumerate(chunks):
                p = Process(target=run_scraper_chunk, args=(
                    i, len(chunks), chunk_urls, args.output_dir, job_timestamp, 
                    args.manufacturer_id, args.product_concurrency,
                    args.sitemap_offset, args.max_sitemaps, args.max_urls_per_sitemap,
                    args.include_row_json
                ))
                processes.append(p)
                p.start()
                logger.info(f"Started process for chunk {i + 1}/{len(chunks)} ({len(chunk_urls)} URLs)")
                
                time.sleep(0.5)
            
            for i, p in enumerate(processes):
                p.join()
                logger.info(f"Completed process for chunk {i + 1}/{len(chunks)}")
            
            end_time = time.time()
            elapsed = end_time - start_time
            
            combined_output = f'{args.output_dir}/output_ashley_{args.manufacturer_id}_{args.job_id}_{timestamp}_combined.csv'
            
            logger.info("="*60)
            logger.info(f"Merging chunk outputs...")
            
            all_dfs = []
            total_products = 0
            
            try:
                import pandas as pd
                has_pandas = True
            except ImportError:
                has_pandas = False
                logger.warning("pandas not installed, skipping merge. Chunk files are available separately.")
            
            if has_pandas:
                for i in range(len(chunks)):
                    chunk_file = f'{args.output_dir}/output_ashley_{args.manufacturer_id}_{job_timestamp}_chunk_{i}.csv'
                    if os.path.exists(chunk_file):
                        try:
                            df = pd.read_csv(chunk_file)
                            all_dfs.append(df)
                            products_in_chunk = len(df)
                            total_products += products_in_chunk
                            logger.info(f"  + Chunk {i + 1}: {products_in_chunk} products")
                        except Exception as e:
                            logger.error(f"  - Failed to read chunk {i + 1}: {e}")
                
                if all_dfs:
                    combined_df = pd.concat(all_dfs, ignore_index=True)
                    combined_df.to_csv(combined_output, index=False)
                    logger.info(f"Combined {total_products} products into {combined_output}")
                else:
                    logger.error("No chunk outputs found!")
                    combined_output = None
            else:
                combined_output = None
                logger.info("Chunk files are available in the output directory:")
                for i in range(len(chunks)):
                    chunk_file = f'{args.output_dir}/output_ashley_{args.manufacturer_id}_{job_timestamp}_chunk_{i}.csv'
                    if os.path.exists(chunk_file):
                        logger.info(f"  - {chunk_file}")
            
            logger.info("="*60)
            logger.info(f"SCRAPE COMPLETED")
            logger.info(f"   Total URLs: {total_urls}")
            logger.info(f"   Parallel chunks: {len(chunks)}")
            logger.info(f"   Time elapsed: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
            if combined_output:
                logger.info(f"   Combined output: {combined_output}")
            logger.info("="*60)
            
            if combined_output:
                print(f"OUTPUT_FILE={combined_output}")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()