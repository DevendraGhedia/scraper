import os
import csv
import time
import sys
import gc
import threading
import requests
import random
import re
import json
import html
import ast
import urllib3
from typing import Optional, List, Dict, Any, Tuple
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse, urljoin

# ================= BYPASS MODULES =================

try:
    from curl_cffi import requests as cc_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False

try:
    from colemanfurniture_brand_file_scraper.utils.proxy_manager import ProxyManager
    HAS_PROXY_MANAGER = True
except Exception:
    HAS_PROXY_MANAGER = False

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.discountlivingrooms.com").rstrip("/")
API_BASE_URL = os.getenv("API_BASE_URL", "https://www.overstock.com/api/product")
SITEMAP_URL = os.getenv("SITEMAP_URL", "").strip()
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))
PROXY_ENV = os.getenv("PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("SITEMAP_PROXY") or ""
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "").rstrip("/")

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= BYPASS & HTTP SESSION =================

class BypassSession:
    """Multi-tiered HTTP client designed to handle Cloudflare and WAF protections."""
    def __init__(self):
        self.proxy = PROXY_ENV
        self.proxy_manager = None
        if not self.proxy and HAS_PROXY_MANAGER:
            try:
                self.proxy_manager = ProxyManager()
            except Exception:
                self.proxy_manager = None

        self.scraper = None
        if HAS_CLOUDSCRAPER:
            try:
                self.scraper = cloudscraper.create_scraper(
                    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
                    delay=10
                )
            except Exception as e:
                log(f"Failed to initialize cloudscraper: {e}", "WARNING")

        self.session = requests.Session()
        self.browser_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive",
        }
        self.session.headers.update(self.browser_headers)

    def _get_proxies_dict(self):
        p = self.proxy
        if not p and self.proxy_manager:
            try:
                if hasattr(self.proxy_manager, 'get_proxy'):
                    p = self.proxy_manager.get_proxy(CURR_URL)
                elif hasattr(self.proxy_manager, 'get_proxy_for_homegallery'):
                    p = self.proxy_manager.get_proxy_for_homegallery()
            except Exception:
                p = None
        if p:
            return {"http": p, "https": p}
        return None

    def _rotate_proxy(self):
        if self.proxy_manager:
            if hasattr(self.proxy_manager, 'mark_proxy_failed') and self.proxy:
                self.proxy_manager.mark_proxy_failed(self.proxy)
            try:
                if hasattr(self.proxy_manager, 'get_proxy'):
                    self.proxy = self.proxy_manager.get_proxy(CURR_URL)
                elif hasattr(self.proxy_manager, 'get_proxy_for_homegallery'):
                    self.proxy = self.proxy_manager.get_proxy_for_homegallery()
                log(f"Rotated proxy to: {self.proxy}", "DEBUG")
            except Exception:
                pass

    def _is_challenge(self, text: str) -> bool:
        if not text:
            return False
        sample = text[:2000].lower()
        return ("<title>just a moment...</title>" in sample or 
                "challenge-platform" in sample or 
                "cf-turnstile" in sample or
                "human verification" in sample or
                "please wait..." in sample)

    def _fetch_flaresolverr(self, url: str, timeout: int = 60) -> Optional[str]:
        try:
            log(f"Requesting {url} via FlareSolverr ({FLARESOLVERR_URL})...", "DEBUG")
            payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": timeout * 1000,
            }
            if self.proxy:
                payload["proxy"] = {"url": self.proxy}
            endpoint = f"{FLARESOLVERR_URL}/v1" if not FLARESOLVERR_URL.endswith("/v1") else FLARESOLVERR_URL
            res = requests.post(endpoint, json=payload, timeout=timeout + 15)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "ok":
                    solution = data.get("solution", {})
                    resp_text = solution.get("response", "")
                    if solution.get("status") == 200 and not self._is_challenge(resp_text):
                        log(f"Success fetching {url} via FlareSolverr", "DEBUG")
                        return resp_text
                    else:
                        log(f"FlareSolverr returned status {solution.get('status')} or challenge page", "WARNING")
                else:
                    log(f"FlareSolverr error response: {data.get('message')}", "WARNING")
        except Exception as e:
            log(f"FlareSolverr request error for {url}: {e}", "WARNING")
        return None

    def fetch(self, url: str, is_json: bool = False, max_retries: int = 3, timeout: int = 25) -> Optional[str]:
        if not url or not isinstance(url, str) or not url.startswith("http"):
            return None

        if FLARESOLVERR_URL:
            content = self._fetch_flaresolverr(url, timeout)
            if content:
                return content

        headers = dict(self.browser_headers)
        if is_json:
            headers.update({
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{CURR_URL}/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            })

        for attempt in range(max_retries):
            proxies = self._get_proxies_dict()

            # Strategy 1: curl_cffi with browser impersonation
            if HAS_CURL_CFFI:
                for imp in ["chrome124", "chrome120", "chrome110"]:
                    try:
                        resp = cc_requests.get(
                            url,
                            headers=headers,
                            impersonate=imp,
                            proxies=proxies,
                            timeout=timeout,
                            verify=False
                        )
                        if resp.status_code == 200 and not self._is_challenge(resp.text):
                            log(f"Success fetching {url} via curl_cffi ({imp})", "DEBUG")
                            return resp.text
                        elif resp.status_code in [403, 429, 503] or self._is_challenge(resp.text):
                            log(f"curl_cffi ({imp}) blocked (status {resp.status_code}) on {url}", "DEBUG")
                    except Exception as e:
                        log(f"curl_cffi ({imp}) error for {url}: {type(e).__name__}", "DEBUG")

            # Strategy 2: cloudscraper
            if self.scraper:
                try:
                    resp = self.scraper.get(url, headers=headers, proxies=proxies, timeout=timeout)
                    if resp.status_code == 200 and not self._is_challenge(resp.text):
                        log(f"Success fetching {url} via cloudscraper", "DEBUG")
                        return resp.text
                    elif resp.status_code in [403, 429, 503] or self._is_challenge(resp.text):
                        log(f"cloudscraper blocked (status {resp.status_code}) for {url}", "DEBUG")
                except Exception as e:
                    log(f"cloudscraper error for {url}: {type(e).__name__}", "DEBUG")

            # Strategy 3: Standard requests with browser headers & proxy
            try:
                resp = self.session.get(url, headers=headers, proxies=proxies, timeout=timeout, verify=False)
                if resp.status_code == 200 and not self._is_challenge(resp.text):
                    log(f"Success fetching {url} via requests", "DEBUG")
                    return resp.text
                elif resp.status_code in [403, 429, 503] or self._is_challenge(resp.text):
                    log(f"requests status {resp.status_code} for {url}", "WARNING")
                    self._rotate_proxy()
            except Exception as e:
                log(f"requests error for {url}: {type(e).__name__}", "WARNING")
                self._rotate_proxy()

            time.sleep(1.5 * (attempt + 1))

        return None

bypass_session = BypassSession()

def get_sitemap_url() -> Optional[str]:
    """Retrieve sitemap URL: checks env var, robots.txt, then common fallbacks."""
    # 1. Environment variable override
    if SITEMAP_URL:
        log(f"Using sitemap URL from environment: {SITEMAP_URL}")
        return SITEMAP_URL

    # 2. Try robots.txt
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Attempting to fetch robots.txt from {robots_url}")
    robots_text = bypass_session.fetch(robots_url, timeout=15)
    if robots_text:
        for line in robots_text.splitlines():
            line_str = line.strip()
            if line_str.lower().startswith("sitemap:"):
                sitemap_val = line_str.split(":", 1)[1].strip()
                if sitemap_val.startswith("http"):
                    log(f"Extracted Sitemap URL from robots.txt: {sitemap_val}")
                    return sitemap_val

    log("Could not extract sitemap from robots.txt (blocked or directive missing), checking common paths...", "WARNING")

    # 3. Fallback common paths (including /sitemaps/sitemap_discount.xml)
    domain_part = urlparse(CURR_URL).netloc.replace("www.", "").split(".")[0]
    candidates = [
        f"{CURR_URL}/sitemaps/sitemap_discount.xml",
        f"{CURR_URL}/sitemaps/sitemap_{domain_part}.xml",
        f"{CURR_URL}/sitemaps/sitemap_tvstands.xml",
        f"{CURR_URL}/sitemaps/sitemap_dining.xml",
        f"{CURR_URL}/sitemaps/sitemap_bedroom.xml",
        f"{CURR_URL}/sitemaps/sitemap.xml",
        f"{CURR_URL}/sitemap.xml",
        f"{CURR_URL}/sitemap_index.xml",
        f"{CURR_URL}/sitemap/sitemap.xml",
        f"{CURR_URL}/pub/media/sitemap.xml"
    ]
    for candidate in candidates:
        log(f"Probing candidate sitemap path: {candidate}", "DEBUG")
        content = bypass_session.fetch(candidate, timeout=15)
        if content and ("<loc>" in content or "<urlset" in content or "<sitemapindex" in content):
            log(f"Found active sitemap candidate: {candidate}")
            return candidate

    return None

def http_get(url: str, is_json: bool = False) -> Optional[str]:
    """Compatibility wrapper around bypass_session.fetch"""
    return bypass_session.fetch(url, is_json=is_json)



def _clean_strings(obj):
    """Recursively clean JSON-escaped strings like \\/"""
    if isinstance(obj, dict):
        return {k: _clean_strings(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_clean_strings(v) for v in obj]

    if isinstance(obj, str):
        # JSON escape cleanup (safe + no warnings)
        return obj.replace('\\/', '/')

    return obj


def extract_datalayer(html_text):
    match = re.search(r'dataLayer\s*=\s*(\[[\s\S]*?\]);', html_text)
    if not match:
        return None

    raw = html.unescape(match.group(1))

    # JS → Python literal compatibility
    raw = raw.replace(':true', ':True') \
             .replace(':false', ':False') \
             .replace(':null', ':None')

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = ast.literal_eval(raw)

    # 🔥 Clean ALL strings at source
    return _clean_strings(data)

def extract_additional_product_info(html_text):
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        
        container = soup.find('div', class_='Product__additional-container')
        
        if not container:
            container = soup.find('div', class_='data-table')
            if not container:
                return json.dumps({})
        
        additional_info = {}

        labels = container.find_all('div', class_='label')
        
        for label in labels:
            label_text = label.get_text(strip=True)
            
            data_div = label.find_next_sibling('div', class_='data')
            
            if data_div:
                data_text = data_div.get_text(strip=True)
                if label_text and data_text:
                    json_key = re.sub(r'[^a-zA-Z0-9_]', '_', label_text.lower().replace(' ', '_'))
                    additional_info[json_key] = data_text

        return json.dumps(additional_info, ensure_ascii=False)
    except Exception as e:
        print(f"Error while processing additional Data: {e}")
        return json.dumps({})

def fetch_json(url: str) -> Optional[dict]:
    """Fetch product page HTML using bypass engine and extract dataLayer + additional product info"""
    try:
        html_content = bypass_session.fetch(url, is_json=False, timeout=25)
        if not html_content:
            log(f"No HTML content fetched for product {url}", "WARNING")
            return None

        data_layer = extract_datalayer(html_content)
        additional_info = extract_additional_product_info(html_content)

        if not data_layer:
            log(f"No dataLayer found in product page: {url}", "DEBUG")
            return None

        product_data = data_layer[0]
        product_data["additional_product_info_html"] = additional_info
        product_data["_raw_html"] = html_content  # pass raw HTML for fallback parsing

        return product_data
    except Exception as e:
        log(f"Error fetching product data for {url}: {e}", "ERROR")
        return None

# ================= SITEMAP PROCESSING =================

def load_xml(url: str) -> Optional[ET.Element]:
    """Load XML sitemap with appropriate anti-bot headers and retry logic"""
    if not url or not isinstance(url, str):
        log(f"Invalid sitemap URL passed to load_xml: {url}", "ERROR")
        return None

    data = bypass_session.fetch(url, is_json=False, timeout=30)
    if not data:
        log(f"Failed to load XML from {url}", "ERROR")
        return None
        
    try:
        # Clean XML if needed
        if "<?xml" not in data[:100]:
            data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parsing failed for {url}: {e}, attempting regex extraction", "WARNING")
        # Try to extract URLs with regex
        try:
            root = ET.Element("urlset")
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', data)
            if not urls:
                log("Regex extraction found no loc tags in sitemap data", "ERROR")
                return None
            for url_text in urls:
                url_elem = ET.SubElement(root, "url")
                loc_elem = ET.SubElement(url_elem, "loc")
                loc_elem.text = url_text.strip()
            return root
        except Exception as e2:
            log(f"Regex extraction also failed: {e2}", "ERROR")
            return None

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    """Normalize image URL for Overstock"""
    if not url:
        return ""
    
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CURR_URL}{url}"
    elif not url.startswith("http"):
        return f"https://ak1.ostkcdn.com{url}" if 'ostkcdn.com' not in url else f"https://{url}"
    
    return url

from typing import Dict

def extract_product_data(product_data: dict) -> dict:
    """
    Extract product data from Discount Living Rooms JSON structure
    """
    try:
        # ---------- Basic ----------
        product_id = str(product_data.get('magentoProductId', ''))
        name = product_data.get('magentoProductName', '').strip()
        sku = product_data.get('magentoProductSku', '')
        
        # ---------- Brand ----------
        # Extract brand from ecommerce items
        ecommerce_items = product_data.get('ecommerce', {}).get('items', [])
        brand = ''
        quantity = 0
        if ecommerce_items:
            brand = ecommerce_items[0].get('item_brand', '')
            quantity = ecommerce_items[0].get('quantity', '')
        
        # ---------- Price ----------
        price = product_data.get('magentoProductPrice', '')
        
        # ---------- Main Image ----------
        main_image = product_data.get('magentoProductImage1', '')

        # ---------- Additional Info ----------
        additional_data = product_data.get('additional_product_info_html', '')
        raw_html = product_data.get('_raw_html', '')
        mpn = sku  # default fallback
        category = ''

        try:
            additional_info_dict = json.loads(additional_data) if additional_data else {}

            # 1️⃣ Prefer Item Number from parsed JSON
            item_number = additional_info_dict.get('item_number', '').strip()
            if item_number:
                mpn = item_number

            category = additional_info_dict.get('product_type', '').strip()

            # 2️⃣ Fallback: Extract directly from HTML if still empty
            if not item_number and raw_html:
                soup = BeautifulSoup(raw_html, "html.parser")
                for label in soup.select("div.label"):
                    if label.get_text(strip=True).lower() == "item number":
                        data_div = label.find_next_sibling("div", class_="data")
                        if data_div:
                            html_item_number = data_div.get_text(strip=True)
                            if html_item_number:
                                mpn = html_item_number
                                break

        except Exception as e:
            print(f"Error setting mpn or category : {e}")
        
        
        # ---------- Category ----------
        category_url = ''
        
        if ecommerce_items and not category:
            # Use the first available category field
            category_fields = [
                'item_category', 'item_category2', 'item_category3',
                'item_category4', 'item_category5', 'item_category6',
                'item_category7', 'item_category8', 'item_category9'
            ]
            categories = []
            for field in category_fields:
                cat_value = ecommerce_items[0].get(field, '')
                if cat_value:  # Only add non-empty values
                    categories.append(cat_value)
            
            # Join all categories with | separator
            if categories:
                category = ' | '.join(categories)
        
        # ---------- Stock Status ----------
        availability = product_data.get('magentoProductAvailability', '')
        status = 'OUT_OF_STOCK'
        
        if availability == 'InStock':
            status = 'SELLABLE'

        
        # ---------- Variation ID ----------
        # Use product ID as variation ID since no separate variation field
        variation_id = ''
        
        # ---------- Additional Attributes ----------
        group_attr_1 = ''
        group_attr_2 = ''
        
        # Could use some product name parts or other fields if needed
        # For now leaving empty as not specified in JSON
        
        return {
            'product_id': product_id,
            'name': name,
            'brand': brand,
            'price': price,
            'main_image': main_image,
            'sku': sku,
            'mpn': mpn,
            'category': category,
            'category_url': category_url,
            'quantity': quantity,
            'status': status,
            'variation_id': variation_id,
            'group_attr_1': group_attr_1,
            'group_attr_2': group_attr_2,
            'additional_data': additional_data,
        }
        
    except Exception as e:
        print(f"Error extracting product data: {e}")
        return {}

def process_product_data(product_url: str, writer, seen: set, stats: dict):
    """Process a single Overstock product URL"""
    if product_url in seen:
        return
    seen.add(product_url)
    
    log(f"Processing product URL: {product_url}", "DEBUG")
    
    data = fetch_json(product_url)  # This uses JSON-specific headers

    if not data:
        stats['errors'] += 1
        log(f"No data found for product {product_url}", "ERROR")
        return
    
    # Extract data from response
    product_info = extract_product_data(data)
    product_id = product_info.get('product_id')
    if not product_id:
        stats['errors'] += 1
        log(f"Invalid data for product {product_url}", "ERROR")
        return
    
    try:
        # Prepare row data
        row = [
            product_url,
            product_info['product_id'],  # Ref Product ID
            product_info['variation_id'],  # Ref Varient ID
            product_info['category'],  # Ref Category
            product_info['category_url'],  # Ref Category URL
            product_info['brand'],  # Ref Brand Name
            product_info['name'],  # Ref Product Name
            product_info['sku'],  # Ref SKU
            product_info['mpn'],  # Ref MPN
            '',  # Ref GTIN (empty for now)
            product_info['price'],  # Ref Price
            normalize_image_url(product_info['main_image']),  # Ref Main Image
            product_info['quantity'],  # Ref Quantity
            product_info['group_attr_1'],  # Ref Group Attr 1
            product_info['group_attr_2'],  # Ref Group Attr 2
            product_info['status'],  # Ref Status
            product_info['additional_data'],  #additional Data
            SCRAPED_DATE  # Date Scrapped
        ]
        
        with csv_lock:
            writer.writerow(row)
        
        stats['products_fetched'] += 1
        log(f"Fetched product {product_info['product_id']}: {product_info['name'][:50]}...", "INFO")
        
    except Exception as e:
        log(f"Error creating row for product {product_id}: {e}", "ERROR")
        stats['errors'] += 1
    
    # Respect request delay
    time.sleep(REQUEST_DELAY)
    stats['urls_processed'] += 1

# ================= MAIN =================

def main():
    sitemap = get_sitemap_url()
    log("=" * 60)
    log("Overstock / DRL Parallel Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"API Base URL: {API_BASE_URL}")
    log(f"Sitemap Index: {sitemap}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY}s")
    log(f"curl_cffi Available: {HAS_CURL_CFFI}")
    log(f"cloudscraper Available: {HAS_CLOUDSCRAPER}")
    log(f"FlareSolverr Configured: {FLARESOLVERR_URL or 'No'}")
    log(f"Proxy Configured: {'Yes' if PROXY_ENV else 'No (auto-detection)'}")
    log("=" * 60)
    
    if not sitemap:
        log("=" * 60, "ERROR")
        log(f"ERROR: Could not locate a valid sitemap for {CURR_URL}.", "ERROR")
        log("The website may be protected by Cloudflare bot challenge/captcha.", "ERROR")
        log("Please provide one of the following:", "ERROR")
        log(" 1. Pass SITEMAP_URL directly via workflow inputs or environment variable.", "ERROR")
        log(" 2. Provide a residential proxy via PROXY / HTTPS_PROXY secrets.", "ERROR")
        log(" 3. Run FlareSolverr and pass FLARESOLVERR_URL.", "ERROR")
        log("=" * 60, "ERROR")
        sys.exit(1)
    
    # Load sitemap index
    log(f"Loading sitemap index from {sitemap}")
    index = load_xml(sitemap)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)
    
    # Extract sitemap URLs
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    
    # Try different XML structures
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements 
            if e.text and "product" in e.text.lower()]
            if sitemaps:
                break
    
    # If no product sitemap found, check if sitemap is directly a urlset or has any sitemaps
    if not sitemaps:
        url_elements = index.findall(".//ns:url/ns:loc", ns) if index.find(".//ns:url", ns) is not None else index.findall(".//url/loc")
        if url_elements:
            log("Sitemap is a direct product URL set rather than an index")
            sitemaps = [sitemap]
        else:
            all_locs = [e.text.strip() for e in index.findall(".//loc") if e.text]
            sitemaps = all_locs if all_locs else [sitemap]
    
    # If still no sitemaps, try regex
    if not sitemaps:
        log("No sitemaps found with XML parsing, trying regex", "WARNING")
    
    # Apply offset and limit
    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)
    
    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]
    
    log(f"Total sitemaps found: {len(sitemaps)}")
    log(f"Sitemaps to process: {len(sitemaps_to_process)}")
    
    # Initialize CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Varient ID",
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
            "Additional Product Data",
            "Date Scrapped"
        ])
        
        # Initialize tracking
        seen = set()
        all_product_urls: list = []  # track all sitemap URLs across all sitemaps
        stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0
        }
        
        # Process each sitemap
        for sitemap_url in sitemaps_to_process:
            stats['sitemaps_processed'] += 1
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")
            
            # Load product sitemap - NO HEADERS for sitemap
            xml = load_xml(sitemap_url)
            if not xml:
                log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                continue
            
            # Extract product URLs - Overstock product URLs typically contain /product/ or /catalog/
            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text
                        and not any(ext in e.text for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
                        and (
                            '.html' in e.text
                        )
                    ]
                    if urls:
                        all_product_urls.extend(urls)  # track sitemap URLs
                        break
            
            if not urls:
                log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                continue
            
            # Apply URL limit
            if MAX_URLS_PER_SITEMAP > 0:
                original_count = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Limited to {len(urls)} out of {original_count} URLs")
            else:
                log(f"Found {len(urls)} product URLs in this sitemap")
            
            # Process URLs in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        stats['errors'] += 1
            
            # Clean up memory
            gc.collect()
    
    # Print statistics
    log("=" * 60)
    log("SCRAPING STATISTICS")
    log("=" * 60)
    log(f"Sitemaps processed: {stats['sitemaps_processed']}")
    log(f"URLs processed: {stats['urls_processed']}")
    log(f"Products successfully fetched: {stats['products_fetched']}")
    log(f"Errors encountered: {stats['errors']}")
    if stats['urls_processed'] > 0:
        success_rate = (stats['products_fetched'] / stats['urls_processed']) * 100
        log(f"Success rate: {success_rate:.1f}%")
    log("=" * 60)
    log(f"Completed: {OUTPUT_CSV}")
    log("=" * 60)

    # Save per-chunk URL list for artifact upload
    if all_product_urls:
        _url_list_file = f"url_list_chunk_{SITEMAP_OFFSET}.txt"
        try:
            with open(_url_list_file, "w", encoding="utf-8") as _uf:
                _uf.write("\n".join(all_product_urls))
            log(f"[URL-LIST] Saved {len(all_product_urls)} URLs to {_url_list_file}")
        except Exception as _exc:
            log(f"[URL-LIST] Warning: {_exc}", "WARNING")

if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Validate environment variables
    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)
    
    main()