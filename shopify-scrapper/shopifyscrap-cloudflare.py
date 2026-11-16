import os
import csv
import time
import sys
import gc
import random
import threading
import cloudscraper
from curl_cffi import requests as cc_requests
from typing import Optional, Tuple
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "").rstrip("/")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))

# Reduced workers to avoid detection
MAX_WORKERS = min(int(os.getenv("MAX_WORKERS", "4")), 6)  # Max 6 workers
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")  # Fixed deprecated utcnow()

# ================= LOGGER =================

def log(msg: str):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

# ================= REQUEST MANAGER =================

class RequestManager:
    def __init__(self):
        # Initialize cloudscraper with browser-like headers
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=10  # Cloudflare challenge delay
        )
        
        # Enhanced headers for both scrapers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            # "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": CURR_URL + "/",
        }
        
        self.scraper.headers.update(self.headers)
        self.retry_delays = [1, 2, 4, 8, 16]  # Exponential backoff
        self.request_count = 0
        self.last_request_time = 0
        self.rate_lock = threading.Lock()
        self.session_lock = threading.Lock()
        
    def _respect_rate_limit(self, crawl_delay=None):
        """Add random delay between requests"""
        with self.rate_lock:
            current_time = time.time()
            if self.request_count > 0:
                elapsed = current_time - self.last_request_time
                base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
                min_delay = base_delay * 0.8
                max_delay = base_delay * 1.5
                target_delay = random.uniform(min_delay, max_delay)
                
                if elapsed < target_delay:
                    sleep_time = target_delay - elapsed
                    time.sleep(sleep_time)
            
            self.last_request_time = time.time()
            self.request_count += 1
            
            if self.request_count % 20 == 0:
                long_pause = random.uniform(8, 15)
                log(f"Taking longer pause after {self.request_count} requests: {long_pause:.1f}s")
                time.sleep(long_pause)
    
    def _fetch_with_cloudscraper(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use cloudscraper for Cloudflare-protected pages"""
        try:
            self._respect_rate_limit(crawl_delay)
            with self.session_lock:
                response = self.scraper.get(url, timeout=45)
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Cloudscraper error for {url}: {e}")
            return None, 0
    
    def _fetch_with_curl_cffi(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use curl_cffi for JavaScript-heavy pages"""
        try:
            self._respect_rate_limit(crawl_delay)
            # Use impersonate to mimic real browser TLS fingerprint
            response = cc_requests.get(
                url, 
                headers=self.headers,
                timeout=45,
                impersonate="chrome110"  # Mimic Chrome 110
            )
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Curl_cffi error for {url}: {e}")
            return None, 0
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None) -> Optional[str]:
        """Intelligent fetching with fallback strategies"""
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        
        # Choose strategy based on retry count
        if retry_count == 0:
            # First try: cloudscraper (best for Cloudflare)
            content, status = self._fetch_with_cloudscraper(url, crawl_delay)
        elif retry_count % 2 == 1:
            # Odd retries: curl_cffi
            content, status = self._fetch_with_curl_cffi(url, crawl_delay)
        else:
            # Even retries: cloudscraper again
            content, status = self._fetch_with_cloudscraper(url, crawl_delay)
        
        if content:
            return content
        
        # Handle specific status codes
        if status in [403, 429, 503]:
            cooldowns = [15, 30, 45, 60, 90]
            delay = cooldowns[min(retry_count, len(cooldowns) - 1)] + random.uniform(1, 3)
            log(f"HTTP {status} for {url}, retry {retry_count+1} in {delay:.1f}s (cooldown)")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        elif status == 404:
            log(f"URL not found: {url}")
            return None
        
        # For other errors, retry with delay
        if status != 200:
            delay = self.retry_delays[retry_count]
            log(f"Retry {retry_count+1} for {url} in {delay}s")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        
        return None

# Initialize global request manager
request_manager = RequestManager()

# ================= HTTP FUNCTIONS =================

def http_get(url: str, crawl_delay=None) -> Optional[str]:
    """Wrapper for request manager"""
    return request_manager.fetch(url, crawl_delay=crawl_delay)

def load_xml(url: str, crawl_delay=None) -> Optional[ET.Element]:
    data = http_get(url, crawl_delay)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}")
        return None

def fetch_json(url: str, crawl_delay=None) -> Optional[dict]:
    data = http_get(url, crawl_delay)
    if not data:
        return None
    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        log(f"JSON decode error for {url}: {e}")
        return None

def normalize_image(url: str) -> str:
    return "https:" + url if url and url.startswith("//") else (url or "")

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def extract_category(tags: list):
    for t in tags:
        if t.startswith("collection_"):
            return t.replace("collection_", ""), ""
    return "", ""

def is_valid_product_url(url: str) -> bool:
    if not url:
        return False
    u = url.strip()
    if "/products/" not in u:
        return False
    clean = u.split("?")[0].rstrip("/")
    if clean.endswith("/products"):
        return False
    return True

def process_product(url: str, writer, seen: set, crawl_delay=None):
    if not is_valid_product_url(url):
        return
        
    clean_url = url.split("?")[0].rstrip("/")
    if clean_url in seen:
        return
    seen.add(clean_url)
    
    product_url = clean_url + ".js"
    product = fetch_json(product_url, crawl_delay)
    
    if not product:
        log(f"Failed to fetch product: {product_url}")
        return
        
    if not product.get("variants"):
        log(f"No variants for product: {product_url}")
        return

    tags = product.get("tags", [])
    category, category_url = extract_category(tags)
    if not category:
        category = product.get("type", "")
    
    brand = product.get("vendor", "")
    product_name = product.get("title", "")
    product_id = product.get("id", "")
    main_image = normalize_image(product.get("featured_image"))
    product_page_url = f"{CURR_URL}{product.get('url', '')}"

    variants_processed = 0
    for v in product["variants"]:
        row = [
            f"{product_page_url}?variant={v.get('id', '')}",  # Ref Product URL
            product_id,                         # Ref Product ID
            v.get("id", ""),                    # Ref Variant ID
            category,                           # Ref Category
            category_url,                       # Ref Category URL
            brand,                              # Ref Brand Name
            product_name,                       # Ref Product Name
            v.get("sku", ""),                   # Ref SKU
            v.get("sku", ""),                   # Ref MPN
            v.get("barcode", ""),               # Ref GTIN
            v.get("price", ""),                 # Ref Price
            main_image,                         # Ref Main Image
            1 if v.get("available") else 0,     # Ref Quantity
            v.get("option1", ""),               # Ref Group Attr 1
            v.get("option2", ""),               # Ref Group Attr 2
            "active" if v.get("available") else "inactive",  # Ref Status
            SCRAPED_DATE                        # Date Scraped
        ]

        with csv_lock:
            writer.writerow(row)
        variants_processed += 1
    
    log(f"Processed {variants_processed} variants from {product_url}")
    
    # Variable delay between products - respect crawl delay if set
    base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
    delay = random.uniform(base_delay * 0.8, base_delay * 1.5)
    time.sleep(delay)

# ================= ROBOTS.TXT CHECK =================

def check_robots_txt():
    """Check robots.txt for crawl delays and sitemap location"""
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    
    robots_content = http_get(robots_url)
    if robots_content:
        lines = robots_content.split('\n')
        crawl_delay = None
        sitemap_url = None
        
        for line in lines:
            line = line.strip()
            # Handle sitemap entries - correctly parse the full URL
            if line.lower().startswith('sitemap:'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    potential_url = parts[1].strip()
                    # Validate it's a proper URL
                    if potential_url.startswith('http'):
                        sitemap_url = potential_url
                        log(f"Found valid sitemap in robots.txt: {sitemap_url}")
            # Handle crawl-delay entries
            elif line.lower().startswith('crawl-delay:'):
                try:
                    parts = line.split(':', 1)
                    if len(parts) > 1:
                        crawl_delay = float(parts[1].strip())
                        log(f"Found Crawl-delay: {crawl_delay} seconds")
                except (ValueError, IndexError) as e:
                    log(f"Error parsing crawl-delay: {e}")
        
        return crawl_delay, sitemap_url
    
    log("No robots.txt found or couldn't fetch it")
    return None, None

# ================= MAIN =================

def main():
    log("Enhanced Cloudflare-resistant scraper started")
    log(f"Base URL: {CURR_URL}")
    log(f"Using cloudscraper + curl_cffi for bypass")
    
    # Check robots.txt
    crawl_delay, robots_sitemap = check_robots_txt()
    
    # Validate the sitemap URL from robots.txt
    sitemap_index = SITEMAP_INDEX  # Default to standard sitemap
    
    if robots_sitemap and robots_sitemap.startswith('http'):
        sitemap_index = robots_sitemap
        log(f"Using sitemap from robots.txt: {sitemap_index}")
    else:
        if robots_sitemap:
            log(f"Invalid sitemap URL in robots.txt: '{robots_sitemap}', using default")
        else:
            log(f"No valid sitemap in robots.txt, using default: {sitemap_index}")
    
    # If crawl_delay is found in robots.txt, use it (but cap it at reasonable value)
    if crawl_delay:
        if crawl_delay > 30:  # Cap at 30 seconds max
            log(f"Crawl-delay {crawl_delay}s is too high, capping at 30s")
            crawl_delay = 30
        log(f"Respecting crawl-delay: {crawl_delay} seconds between requests")
    else:
        log(f"Using default request delay: {REQUEST_DELAY_BASE} seconds")
    
    # Load sitemap index
    log(f"Loading sitemap index: {sitemap_index}")
    index = load_xml(sitemap_index, crawl_delay)
    if not index:
        log("Failed to load sitemap index")
        sys.exit(1)
    
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = [e.text for e in index.findall(".//ns:sitemap/ns:loc", ns)]
    
    if not sitemaps:
        # Try alternative namespace
        sitemaps = [e.text for e in index.findall(".//sitemap/loc")]
        if not sitemaps:
            # Try without namespace as last resort
            sitemaps = [e.text for e in index.findall(".//loc")]
    
    prod_sitemaps = [s for s in sitemaps if s and "product" in s.lower()]
    if prod_sitemaps:
        sitemaps = prod_sitemaps
        log(f"Filtered to {len(sitemaps)} product sitemaps")
    else:
        log(f"Total sitemaps found: {len(sitemaps)}")
    
    # Apply offset and limit
    if MAX_SITEMAPS > 0:
        sitemaps = sitemaps[SITEMAP_OFFSET:SITEMAP_OFFSET + MAX_SITEMAPS]
    elif SITEMAP_OFFSET > 0:
        sitemaps = sitemaps[SITEMAP_OFFSET:]
    
    log(f"Sitemaps to process in this chunk: {len(sitemaps)}")
    
    if not sitemaps:
        log("No sitemaps to process")
        sys.exit(0)
    
    # Create output file
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Variant ID",
            "Ref Category",
            "Ref Category URL",
            "Ref Brand Name",
            "Ref Product Name",
            "Ref SKU",
            "Ref MPN",
            "Ref GTIN",
            "Ref Price",
            "Ref Main Image",
            "Ref Quantity",
            "Ref Group Attr 1",
            "Ref Group Attr 2",
            "Ref Status",
            "Date Scraped"
        ])
        
        seen = set()
        total_products = 0
        all_product_urls: list = []  # track all URLs fetched from sitemaps
        
        # Process sitemaps
        for sitemap_idx, sitemap_url in enumerate(sitemaps):
            log(f"[{sitemap_idx+1}/{len(sitemaps)}] Loading sitemap: {sitemap_url}")
            
            xml = load_xml(sitemap_url, crawl_delay)
            if not xml:
                log(f"  Failed to load sitemap, skipping")
                continue
            
            # Extract URLs
            urls = [e.text for e in xml.findall(".//ns:url/ns:loc", ns)]
            if not urls:
                urls = [e.text for e in xml.findall(".//url/loc")]
            if not urls:
                urls = [e.text for e in xml.findall(".//loc")]
            
            log(f"  Found {len(urls)} URLs in sitemap")
            all_product_urls.extend([u for u in urls if u])
            
            if MAX_URLS_PER_SITEMAP and len(urls) > MAX_URLS_PER_SITEMAP:
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"  Limited to {len(urls)} URLs")
            
            # Filter to only valid product URLs
            urls = [u for u in urls if is_valid_product_url(u)]
            log(f"  Valid product URLs to process: {len(urls)}")

            # Process URLs with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for url in urls:
                    future = executor.submit(process_product, url, writer, seen, crawl_delay)
                    futures.append(future)
                
                # Monitor progress
                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    if completed % 5 == 0:
                        log(f"  Processed {completed}/{len(futures)} URLs in this sitemap")
                    try:
                        future.result()
                    except Exception as e:
                        log(f"  Error processing URL: {e}")
            
            total_products += len(urls)
            
            # Longer pause between sitemaps
            if sitemap_idx < len(sitemaps) - 1:
                base_pause = crawl_delay * 5 if crawl_delay else 10
                pause = random.uniform(base_pause * 0.8, base_pause * 1.2)
                log(f"  Pausing {pause:.1f}s before next sitemap...")
                time.sleep(pause)
            
            gc.collect()
    
    log(f"Chunk completed: {OUTPUT_CSV}")
    log(f"Total unique products processed: {len(seen)}")
    log(f"Total requests made: {request_manager.request_count}")

    # Save per-chunk URL list for artifact upload (merged in YML workflow)
    url_list_file = f"url_list_chunk_{SITEMAP_OFFSET}.txt"
    if all_product_urls:
        try:
            with open(url_list_file, "w", encoding="utf-8") as _uf:
                _uf.write("\n".join(all_product_urls))
            log(f"✓ Saved {len(all_product_urls)} sitemap URLs to {url_list_file}")
        except Exception as exc:
            log(f"Note: Could not save URL list: {exc}")

if __name__ == "__main__":
    main()