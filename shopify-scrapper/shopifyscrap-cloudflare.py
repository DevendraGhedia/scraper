import os
import csv
import time
import sys
import gc
import random
import re
import threading
import requests
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

# Reduced workers to avoid Cloudflare rate-limit detection
MAX_WORKERS = min(int(os.getenv("MAX_WORKERS", "2")), 4)
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", os.getenv("REQUEST_DELAY_BASE", "2.0")))

FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "").rstrip("/")
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "120"))
PROXY = os.getenv("PROXY", os.getenv("HTTPS_PROXY", "")).strip()

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ================= LOGGER =================

def log(msg: str):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

# ================= REQUEST MANAGER =================

class RequestManager:
    def __init__(self):
        self.proxy = PROXY if PROXY else None
        self.flaresolverr_url = FLARESOLVERR_URL if FLARESOLVERR_URL else None
        self.flaresolverr_lock = threading.Lock()
        self.flaresolverr_session_id = None
        self.clearance_cookies = {}
        self.cf_user_agent = None

        # Initialize cloudscraper as fallback
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
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": CURR_URL + "/" if CURR_URL else "https://www.google.com/",
        }
        
        self.scraper.headers.update(self.headers)
        if self.proxy:
            self.proxies = {"http": self.proxy, "https": self.proxy}
            self.scraper.proxies.update(self.proxies)
            log(f"Configured proxy: {self.proxy[:25]}...")
        else:
            self.proxies = None

        if self.flaresolverr_url:
            log(f"Configured FlareSolverr endpoint: {self.flaresolverr_url}")

        self.retry_delays = [2, 5, 12, 25, 45]  # Exponential backoff with longer windows
        self.request_count = 0
        self.last_request_time = 0
        self.rate_lock = threading.Lock()
        self.consecutive_blocks = 0
        self.block_lock = threading.Lock()
        self._thread_local = threading.local()

    def _reset_curl_session(self):
        """Reset thread-local curl session to clear blocked/tainted cookies and session IDs"""
        if hasattr(self._thread_local, "session") and self._thread_local.session:
            try:
                self._thread_local.session.close()
            except Exception:
                pass
            self._thread_local.session = None

    def _get_curl_session(self):
        if not hasattr(self._thread_local, "session") or self._thread_local.session is None:
            session = cc_requests.Session(impersonate="chrome120")
            session.headers.update(self.headers)
            if self.proxies:
                session.proxies = self.proxies
            if self.cf_user_agent:
                session.headers["User-Agent"] = self.cf_user_agent
            if self.clearance_cookies:
                session.cookies.update(self.clearance_cookies)
            self._thread_local.session = session
        else:
            session = self._thread_local.session
            if self.clearance_cookies:
                session.cookies.update(self.clearance_cookies)
            if self.cf_user_agent:
                session.headers["User-Agent"] = self.cf_user_agent
        return session
        
    def _respect_rate_limit(self, crawl_delay=None):
        """Add random delay between requests with thread-safe spacing"""
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
            
            # Periodic longer breathing pause to prevent WAF rate-limiting
            if self.request_count % 40 == 0:
                long_pause = random.uniform(8, 14)
                log(f"Taking rate-limit breathing pause after {self.request_count} requests: {long_pause:.1f}s")
                time.sleep(long_pause)

    def _is_challenge(self, status: int, text: str) -> bool:
        if status in [403, 503]:
            return True
        if not text:
            return False
        sample = text[:2500].lower()
        return ("<title>just a moment...</title>" in sample or 
                "challenge-platform" in sample or 
                "cf-turnstile" in sample or
                "human verification" in sample or
                "please wait..." in sample or
                "attention required! | cloudflare" in sample)

    def _init_flaresolverr_session(self):
        if not self.flaresolverr_url:
            return
        endpoint = f"{self.flaresolverr_url}/v1" if not self.flaresolverr_url.endswith("/v1") else self.flaresolverr_url
        try:
            payload = {"cmd": "sessions.create"}
            if self.proxy:
                payload["proxy"] = {"url": self.proxy}
            res = requests.post(endpoint, json=payload, timeout=25)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "ok":
                    self.flaresolverr_session_id = data.get("session")
                    log(f"Created persistent FlareSolverr session: {self.flaresolverr_session_id}")
        except Exception as e:
            log(f"Could not create persistent FlareSolverr session: {e}")

    def _fetch_with_flaresolverr(self, url: str) -> Optional[Tuple[str, int]]:
        """Solve Cloudflare Turnstile challenge using FlareSolverr service"""
        if not self.flaresolverr_url:
            return None
        
        endpoint = f"{self.flaresolverr_url}/v1" if not self.flaresolverr_url.endswith("/v1") else self.flaresolverr_url
        with self.flaresolverr_lock:
            # Ensure persistent session exists
            if not self.flaresolverr_session_id:
                self._init_flaresolverr_session()

            payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": FLARESOLVERR_TIMEOUT * 1000,
            }
            if self.flaresolverr_session_id:
                payload["session"] = self.flaresolverr_session_id
            if self.proxy:
                payload["proxy"] = {"url": self.proxy}
            
            try:
                log(f"Solving challenge via FlareSolverr for: {url}")
                res = requests.post(endpoint, json=payload, timeout=FLARESOLVERR_TIMEOUT + 30)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("status") == "ok":
                        solution = data.get("solution", {})
                        resp_text = solution.get("response", "")
                        
                        # Extract cookies and user-agent
                        new_cookies = {}
                        for c in solution.get("cookies", []):
                            c_name = c.get("name")
                            c_val = c.get("value")
                            if c_name and c_val:
                                new_cookies[c_name] = c_val
                        
                        ua = solution.get("userAgent")
                        if ua:
                            self.cf_user_agent = ua
                            self.headers["User-Agent"] = ua
                        
                        if new_cookies:
                            self.clearance_cookies.update(new_cookies)
                            log(f"FlareSolverr acquired {len(new_cookies)} clearance cookies (cf_clearance={'cf_clearance' in new_cookies})")
                        
                        return resp_text, 200
                    else:
                        log(f"FlareSolverr response message: {data.get('message')}")
                        self.flaresolverr_session_id = None
                else:
                    log(f"FlareSolverr returned HTTP {res.status_code}: {res.text[:200]}")
            except Exception as e:
                log(f"FlareSolverr request error: {e}")
                self.flaresolverr_session_id = None
        return None
    
    def _fetch_with_curl_cffi(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use curl_cffi with Chrome 120 impersonation to bypass Cloudflare TLS fingerprints"""
        try:
            self._respect_rate_limit(crawl_delay)
            session = self._get_curl_session()
            response = session.get(url, timeout=45)
            if response.status_code == 200 and not self._is_challenge(response.status_code, response.text):
                return response.text, response.status_code
            return response.text if response.text else None, response.status_code
        except Exception as e:
            log(f"Curl_cffi error for {url}: {e}")
            return None, 0

    def _fetch_with_cloudscraper(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use cloudscraper as fallback for Cloudflare-protected pages"""
        try:
            self._respect_rate_limit(crawl_delay)
            response = self.scraper.get(url, timeout=45)
            if response.status_code == 200 and not self._is_challenge(response.status_code, response.text):
                return response.text, response.status_code
            return response.text if response.text else None, response.status_code
        except Exception as e:
            log(f"Cloudscraper error for {url}: {e}")
            return None, 0
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None) -> Optional[str]:
        """Intelligent fetching with FlareSolverr & fallback strategies"""
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        
        # Primary fetch: curl_cffi (Chrome 120 TLS fingerprint)
        content, status = self._fetch_with_curl_cffi(url, crawl_delay)

        # Check if successful
        if content and status == 200 and not self._is_challenge(status, content):
            with self.block_lock:
                self.consecutive_blocks = 0
            return content
        
        # If challenged or 403 / 503, try FlareSolverr and reset session
        if self._is_challenge(status, content or ""):
            log(f"Cloudflare challenge or HTTP {status} detected on {url}")
            self._reset_curl_session()
            
            with self.block_lock:
                self.consecutive_blocks += 1
                current_blocks = self.consecutive_blocks
            
            # If multiple consecutive blocks, apply adaptive cooldown so WAF bucket resets
            if current_blocks >= 3:
                cooldown = random.uniform(25, 40)
                log(f"Multiple consecutive blocks ({current_blocks}). Adaptive cooldown pause for {cooldown:.1f}s...")
                time.sleep(cooldown)
                with self.block_lock:
                    self.consecutive_blocks = 0

            if self.flaresolverr_url:
                fs_result = self._fetch_with_flaresolverr(url)
                if fs_result:
                    fs_content, fs_status = fs_result
                    if fs_content and fs_status == 200 and not self._is_challenge(fs_status, fs_content):
                        with self.block_lock:
                            self.consecutive_blocks = 0
                        return fs_content
            
            # If 429 (rate-limited) or 403, apply cooldown sleep so Cloudflare/Shopify bucket resets
            if status in [429, 403]:
                cooldown = random.uniform(15, 25)
                log(f"Throttled (HTTP {status}). Cooldown sleep for {cooldown:.1f}s...")
                time.sleep(cooldown)

        # Fallback to cloudscraper
        if retry_count % 2 == 1:
            cs_content, cs_status = self._fetch_with_cloudscraper(url, crawl_delay)
            if cs_content and cs_status == 200 and not self._is_challenge(cs_status, cs_content):
                with self.block_lock:
                    self.consecutive_blocks = 0
                return cs_content

        if status == 404:
            log(f"URL not found (404): {url}")
            return None
        
        # Retry with progressive backoff
        delay = self.retry_delays[retry_count] + random.uniform(1, 3)
        log(f"Retry {retry_count+1}/{len(self.retry_delays)} for {url} in {delay:.1f}s (status={status})")
        time.sleep(delay)
        return self.fetch(url, retry_count + 1, crawl_delay)

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
        log(f"XML parse error for {url}: {e}, attempting entity cleanup")
        try:
            cleaned = re.sub(r'&(?!(?:amp|lt|gt|quot|apos);)', '&amp;', data)
            return ET.fromstring(cleaned)
        except Exception:
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

def format_price(raw_price) -> str:
    if raw_price is None or raw_price == "":
        return ""
    try:
        # Shopify AJAX endpoint (.js) returns price in cents as integer (e.g. 48300 -> 483.00)
        # while .json returns decimal string (e.g. "483.00")
        if isinstance(raw_price, int):
            return f"{raw_price / 100.0:.2f}"
        s = str(raw_price).strip()
        if "." not in s and s.isdigit():
            return f"{int(s) / 100.0:.2f}"
        return f"{float(s):.2f}"
    except Exception:
        return str(raw_price)

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def extract_category(tags: list):
    for t in tags:
        if t.startswith("collection_"):
            return t.replace("collection_", ""), ""
    return "", ""

def process_product(url: str, writer, seen: set, crawl_delay=None, file_handle=None):
    if url in seen:
        return
    seen.add(url)
    
    product_url = url.rstrip("/") + ".js"
    product = fetch_json(product_url, crawl_delay)
    
    # Fallback to .json endpoint if .js failed or returned empty
    if not product or not product.get("variants"):
        json_url = url.rstrip("/") + ".json"
        json_data = fetch_json(json_url, crawl_delay)
        if json_data and isinstance(json_data, dict) and "product" in json_data:
            product = json_data["product"]

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
            format_price(v.get("price")),       # Ref Price
            main_image,                         # Ref Main Image
            1 if v.get("available") else 0,     # Ref Quantity
            v.get("option1", ""),               # Ref Group Attr 1
            v.get("option2", ""),               # Ref Group Attr 2
            "active" if v.get("available") else "inactive",  # Ref Status
            SCRAPED_DATE                        # Date Scraped
        ]

        with csv_lock:
            writer.writerow(row)
            if file_handle:
                file_handle.flush()
        variants_processed += 1
    
    log(f"Processed {variants_processed} variants from {product_url}")

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
            # Handle sitemap entries - cleanly extract URL with regex to avoid trailing text/comments
            if line.lower().startswith('sitemap:'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    match = re.search(r'https?://[^\s]+', parts[1])
                    if match:
                        potential_url = match.group(0).rstrip(';,')
                        sitemap_url = potential_url
                        log(f"Found valid sitemap in robots.txt: {sitemap_url}")
            # Handle crawl-delay entries
            elif line.lower().startswith('crawl-delay:'):
                try:
                    parts = line.split(':', 1)
                    if len(parts) > 1:
                        crawl_delay = float(parts[1].strip().split()[0])
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
    sitemaps = []
    if index is not None:
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        sitemaps = [e.text.strip() for e in index.findall(".//ns:sitemap/ns:loc", ns) if e.text and e.text.strip()]
        if not sitemaps:
            sitemaps = [e.text.strip() for e in index.findall(".//sitemap/loc") if e.text and e.text.strip()]
        if not sitemaps:
            sitemaps = [e.text.strip() for e in index.findall(".//loc") if e.text and e.text.strip()]
    
    # Regex fallback if XML parsing had issues
    if not sitemaps:
        raw_xml = http_get(sitemap_index, crawl_delay)
        if raw_xml:
            sitemaps = [s.strip() for s in re.findall(r'<loc>([^<]+)</loc>', raw_xml) if s.strip()]

    if not sitemaps:
        log("Failed to load sitemap index or extract sitemaps")
        sys.exit(1)
    
    log(f"Total sitemaps found: {len(sitemaps)}")

    # In Shopify stores, filter for product sitemaps to avoid scraping pages/collections/blogs
    product_sitemaps = [s for s in sitemaps if "product" in s.lower()]
    if product_sitemaps:
        log(f"Filtered to {len(product_sitemaps)} product sitemaps (out of {len(sitemaps)} total sitemaps)")
        sitemaps = product_sitemaps
    
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
            if xml is None:
                log(f"  Failed to load sitemap, skipping")
                continue
            
            # Extract URLs
            raw_urls = [e.text for e in xml.findall(".//ns:url/ns:loc", ns)]
            if not raw_urls:
                raw_urls = [e.text for e in xml.findall(".//url/loc")]
            if not raw_urls:
                raw_urls = [e.text for e in xml.findall(".//loc")]
            
            # Filter specifically for product pages and strip whitespace
            urls = [u.strip() for u in raw_urls if u and u.strip() and "/products/" in u]
            
            log(f"  Found {len(urls)} product URLs in sitemap")
            all_product_urls.extend([u for u in urls if u])
            
            if MAX_URLS_PER_SITEMAP and len(urls) > MAX_URLS_PER_SITEMAP:
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"  Limited to {len(urls)} URLs")
            
            # Process URLs with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for url in urls:
                    if url and (url.startswith(CURR_URL) or url.startswith('http')):
                        future = executor.submit(process_product, url, writer, seen, crawl_delay, f)
                        futures.append(future)
                    elif url:
                        log(f"  Skipping URL not from current domain: {url}")
                
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