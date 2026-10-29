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
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "120"))

# Known sitemaps for the 4 sister domains
SITE_SITEMAPS = {
    "discountlivingrooms.com": "https://www.discountlivingrooms.com/sitemaps/sitemap_discount.xml",
    "bedroomfurniturediscounts.com": "https://www.bedroomfurniturediscounts.com/sitemaps/sitemap_bedroom.xml",
    "diningroomsoutlet.com": "https://www.diningroomsoutlet.com/sitemaps/sitemap_dining.xml",
    "tvstandsoutlet.com": "https://www.tvstandsoutlet.com/sitemaps/sitemap_tvstands.xml",
}

SITE_NAME = os.getenv("SITE_NAME", "").strip().lower()
if not SITE_NAME:
    parsed_netloc = urlparse(CURR_URL).netloc.replace("www.", "").split(".")[0]
    SITE_NAME = parsed_netloc or "discountlivingrooms"

OUTPUT_CSV = f"products_{SITE_NAME}_chunk_{SITEMAP_OFFSET}.csv"
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
        self.flaresolverr_session_id = None
        self.clearance_cookies = {}
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

    def _init_flaresolverr_session(self):
        if not FLARESOLVERR_URL:
            return
        endpoint = f"{FLARESOLVERR_URL}/v1" if not FLARESOLVERR_URL.endswith("/v1") else FLARESOLVERR_URL
        try:
            payload = {"cmd": "sessions.create"}
            if self.proxy:
                payload["proxy"] = {"url": self.proxy}
            res = requests.post(endpoint, json=payload, timeout=20)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "ok":
                    self.flaresolverr_session_id = data.get("session")
                    log(f"Created FlareSolverr session: {self.flaresolverr_session_id}", "DEBUG")
        except Exception as e:
            log(f"Could not create persistent FlareSolverr session: {e}", "DEBUG")

    def _fetch_flaresolverr(self, url: str, timeout: int = 120) -> Optional[str]:
        if not FLARESOLVERR_URL:
            return None
        endpoint = f"{FLARESOLVERR_URL}/v1" if not FLARESOLVERR_URL.endswith("/v1") else FLARESOLVERR_URL

        # Ensure session exists
        if not self.flaresolverr_session_id:
            self._init_flaresolverr_session()

        for attempt in range(2):
            try:
                log(f"Requesting {url} via FlareSolverr (timeout={timeout}s, attempt={attempt+1})...", "DEBUG")
                payload = {
                    "cmd": "request.get",
                    "url": url,
                    "maxTimeout": timeout * 1000,
                }
                if self.flaresolverr_session_id:
                    payload["session"] = self.flaresolverr_session_id
                if self.proxy:
                    payload["proxy"] = {"url": self.proxy}

                res = requests.post(endpoint, json=payload, timeout=timeout + 20)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("status") == "ok":
                        solution = data.get("solution", {})
                        resp_text = solution.get("response", "")

                        # Save cookies and userAgent for direct fast requests
                        cookies = solution.get("cookies", [])
                        for cookie in cookies:
                            c_name = cookie.get("name")
                            c_val = cookie.get("value")
                            if c_name and c_val:
                                self.clearance_cookies[c_name] = c_val
                                self.session.cookies.set(c_name, c_val, domain=cookie.get("domain", ""))

                        user_agent = solution.get("userAgent")
                        if user_agent:
                            self.browser_headers["User-Agent"] = user_agent
                            self.session.headers["User-Agent"] = user_agent

                        if solution.get("status") == 200 and not self._is_challenge(resp_text):
                            log(f"Success fetching {url} via FlareSolverr", "DEBUG")
                            return resp_text
                        else:
                            log(f"FlareSolverr returned status {solution.get('status')} or challenge page", "WARNING")
                    else:
                        log(f"FlareSolverr message: {data.get('message')}", "WARNING")
                        self.flaresolverr_session_id = None
            except Exception as e:
                log(f"FlareSolverr attempt {attempt+1} error for {url}: {e}", "WARNING")
                self.flaresolverr_session_id = None

            time.sleep(3)

        return None

    def fetch(self, url: str, is_json: bool = False, max_retries: int = 3, timeout: int = 25) -> Optional[str]:
        if not url or not isinstance(url, str) or not url.startswith("http"):
            return None

        # If we have clearance cookies from FlareSolverr, first try fast direct requests
        if self.clearance_cookies.get("cf_clearance"):
            proxies = self._get_proxies_dict()
            headers = dict(self.browser_headers)
            if is_json:
                headers.update({
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"{CURR_URL}/",
                })

            # Fast Strategy 1: curl_cffi with clearance cookies & Chrome TLS impersonation
            if HAS_CURL_CFFI:
                try:
                    resp = cc_requests.get(
                        url,
                        headers=headers,
                        cookies=self.clearance_cookies,
                        impersonate="chrome124",
                        proxies=proxies,
                        timeout=20,
                        verify=False
                    )
                    if resp.status_code == 200 and not self._is_challenge(resp.text):
                        log(f"Fast curl_cffi request succeeded for {url} using cf_clearance", "DEBUG")
                        return resp.text
                except Exception as e:
                    log(f"Fast curl_cffi request failed: {e}", "DEBUG")

            # Fast Strategy 2: requests.Session with clearance cookies
            try:
                resp = self.session.get(url, headers=headers, proxies=proxies, timeout=15, verify=False)
                if resp.status_code == 200 and not self._is_challenge(resp.text):
                    log(f"Fast direct session request succeeded for {url} using cf_clearance", "DEBUG")
                    return resp.text
            except Exception as e:
                log(f"Fast direct session request failed: {e}", "DEBUG")

        # If FlareSolverr is configured, use it with generous timeout (e.g. 90-120s for sitemaps)
        if FLARESOLVERR_URL:
            fs_timeout = max(timeout, FLARESOLVERR_TIMEOUT)
            content = self._fetch_flaresolverr(url, timeout=fs_timeout)
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

    # 2. Known domain mapping for the 4 sister sites
    domain_clean = urlparse(CURR_URL).netloc.lower().replace("www.", "")
    if domain_clean in SITE_SITEMAPS:
        known_sitemap = SITE_SITEMAPS[domain_clean]
        log(f"Using known sitemap for {domain_clean}: {known_sitemap}")
        return known_sitemap

    # 3. Try robots.txt
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


def extract_datalayer(html_text: str) -> Optional[List[dict]]:
    data_list = []

    # 1. Search for dataLayer = [...]
    for match in re.finditer(r'dataLayer\s*=\s*(\[[\s\S]*?\]);', html_text):
        raw = html.unescape(match.group(1))
        raw = raw.replace(':true', ':True').replace(':false', ':False').replace(':null', ':None')
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                data_list.extend(data)
            elif isinstance(data, dict):
                data_list.append(data)
        except Exception:
            try:
                data = ast.literal_eval(raw)
                if isinstance(data, list):
                    data_list.extend(data)
                elif isinstance(data, dict):
                    data_list.append(data)
            except Exception:
                pass

    # 2. Search for dataLayer.push(...)
    for push_match in re.finditer(r'dataLayer\.push\s*\(\s*(\{[\s\S]*?\})\s*\);', html_text):
        raw = html.unescape(push_match.group(1))
        raw = raw.replace(':true', ':True').replace(':false', ':False').replace(':null', ':None')
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                data_list.append(data)
        except Exception:
            try:
                data = ast.literal_eval(raw)
                if isinstance(data, dict):
                    data_list.append(data)
            except Exception:
                pass

    if data_list:
        return _clean_strings(data_list)
    return None

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

        product_data = {}
        if data_layer:
            if isinstance(data_layer, list):
                # Search through dataLayer entries to find the product info dictionary
                for item in data_layer:
                    if isinstance(item, dict) and any(k in item for k in ['magentoProductId', 'ecommerce', 'ecomm_prodid', 'product', 'item_id']):
                        product_data = dict(item)
                        break
                if not product_data and len(data_layer) > 0 and isinstance(data_layer[0], dict):
                    product_data = dict(data_layer[0])
            elif isinstance(data_layer, dict):
                product_data = dict(data_layer)

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

    data = bypass_session.fetch(url, is_json=False, timeout=FLARESOLVERR_TIMEOUT)
    if not data:
        log(f"Failed to load XML from {url}", "ERROR")
        return None
        
    try:
        # Check if Chrome wrapped XML in an HTML viewer (common with FlareSolverr)
        if '<div id="webkit-xml-viewer-source-xml"' in data:
            match = re.search(r'<div id="webkit-xml-viewer-source-xml"[^>]*>([\s\S]*?)</div>\s*</body>', data)
            if match:
                data = match.group(1).strip()
        elif '<pre' in data and '</pre>' in data:
            match = re.search(r'<pre[^>]*>([\s\S]*?)</pre>', data)
            if match:
                data = match.group(1).strip()

        # Unescape HTML entities if wrapped
        if "&lt;" in data and "&gt;" in data:
            data = html.unescape(data)

        # Clean XML declaration if needed
        if "<?xml" not in data[:100] and not data.strip().startswith("<"):
            data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data

        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parsing failed for {url}: {e}, attempting regex extraction", "WARNING")
        # Try to extract URLs with regex
        try:
            root = ET.Element("urlset")
            clean_text = html.unescape(data)
            urls = re.findall(r'<loc[^>]*>\s*(https?://[^<\s]+)\s*</loc>', clean_text, re.IGNORECASE)
            if not urls:
                # Fallback: find any valid http URLs ending with .xml or .html
                urls = re.findall(r'https?://[^\s<"\'\\]+\.(?:xml|html)', clean_text)
            if not urls:
                log("Regex extraction found no valid URLs in sitemap data", "ERROR")
                return None
            for url_text in urls:
                url_elem = ET.SubElement(root, "url")
                loc_elem = ET.SubElement(url_elem, "loc")
                loc_elem.text = url_text.strip()
            log(f"Regex extraction successfully found {len(urls)} URLs in {url}")
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
    Extract product data from Discount Living Rooms / sister sites JSON structure + HTML fallback
    """
    try:
        if not isinstance(product_data, dict):
            if isinstance(product_data, list) and len(product_data) > 0 and isinstance(product_data[0], dict):
                product_data = product_data[0]
            else:
                product_data = {}

        raw_html = product_data.get('_raw_html', '')

        # ---------- Ecommerce items extraction (handles list, dict, impressions) ----------
        ecommerce = product_data.get('ecommerce')
        ecommerce_items = []
        if isinstance(ecommerce, list):
            ecommerce_items = ecommerce
        elif isinstance(ecommerce, dict):
            items_candidate = ecommerce.get('items') or ecommerce.get('impressions') or ecommerce.get('detail', {}).get('products')
            if isinstance(items_candidate, list):
                ecommerce_items = items_candidate
            elif isinstance(items_candidate, dict):
                ecommerce_items = [items_candidate]

        # ---------- Basic: Product ID ----------
        product_id = str(product_data.get('magentoProductId', '')).strip()
        if not product_id and ecommerce_items and isinstance(ecommerce_items[0], dict):
            product_id = str(ecommerce_items[0].get('item_id', '') or ecommerce_items[0].get('id', '')).strip()
        if not product_id and product_data.get('ecomm_prodid'):
            ecomm_pid = product_data.get('ecomm_prodid')
            product_id = str(ecomm_pid[0] if isinstance(ecomm_pid, list) else ecomm_pid).strip()
        if not product_id and isinstance(product_data.get('product'), dict):
            product_id = str(product_data['product'].get('id', '')).strip()

        # ---------- Product Name ----------
        name = product_data.get('magentoProductName', '').strip()
        if not name and ecommerce_items and isinstance(ecommerce_items[0], dict):
            name = str(ecommerce_items[0].get('item_name') or ecommerce_items[0].get('name') or '').strip()
        if not name and isinstance(product_data.get('product'), dict):
            name = str(product_data['product'].get('name', '')).strip()

        # ---------- SKU ----------
        sku = product_data.get('magentoProductSku', '')
        if not sku and ecommerce_items and isinstance(ecommerce_items[0], dict):
            sku = str(ecommerce_items[0].get('item_sku') or ecommerce_items[0].get('sku') or ecommerce_items[0].get('item_id') or '')
        if not sku:
            sku = product_data.get('ecomm_prodsku', '')
        if not sku and isinstance(product_data.get('product'), dict):
            sku = str(product_data['product'].get('sku', '')).strip()

        # ---------- Brand & Quantity ----------
        brand = ''
        quantity = 0
        if ecommerce_items and isinstance(ecommerce_items[0], dict):
            brand = str(ecommerce_items[0].get('item_brand') or ecommerce_items[0].get('brand') or '')
            quantity = ecommerce_items[0].get('quantity', 0)
        if not brand and isinstance(product_data.get('product'), dict):
            brand = str(product_data['product'].get('brand', ''))
        if not brand:
            brand = str(product_data.get('brand', ''))

        # ---------- Price ----------
        price = str(product_data.get('magentoProductPrice', '')).strip()
        if not price and ecommerce_items and isinstance(ecommerce_items[0], dict):
            price = str(ecommerce_items[0].get('price', '')).strip()
        if not price and isinstance(ecommerce, dict):
            price = str(ecommerce.get('value', '')).strip()
        if not price and product_data.get('ecomm_totalvalue'):
            price = str(product_data.get('ecomm_totalvalue')).strip()

        # ---------- Main Image ----------
        main_image = product_data.get('magentoProductImage1', '')
        if not main_image and isinstance(product_data.get('product'), dict):
            main_image = product_data['product'].get('image', '')
        if not main_image and ecommerce_items and isinstance(ecommerce_items[0], dict):
            main_image = ecommerce_items[0].get('item_image') or ecommerce_items[0].get('image') or ''

        # ---------- Additional Info & MPN ----------
        additional_data = product_data.get('additional_product_info_html', '')
        mpn = sku
        category = ''

        try:
            additional_info_dict = json.loads(additional_data) if additional_data else {}
            item_number = additional_info_dict.get('item_number', '').strip()
            if item_number:
                mpn = item_number
            category = additional_info_dict.get('product_type', '').strip()

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
            log(f"Error setting mpn or category : {e}", "DEBUG")

        # ---------- Category from Ecommerce items ----------
        if ecommerce_items and isinstance(ecommerce_items[0], dict) and not category:
            category_fields = [
                'item_category', 'item_category2', 'item_category3',
                'item_category4', 'item_category5', 'item_category6',
                'item_category7', 'item_category8', 'item_category9'
            ]
            categories = []
            for field in category_fields:
                cat_value = ecommerce_items[0].get(field, '')
                if cat_value:
                    categories.append(str(cat_value))
            if categories:
                category = ' | '.join(categories)

        category_url = ''

        # ---------- HTML Fallback for missing fields ----------
        if raw_html:
            soup = BeautifulSoup(raw_html, "html.parser")

            if not product_id:
                prod_inp = soup.find('input', {'name': 'product'})
                if prod_inp and prod_inp.get('value'):
                    product_id = prod_inp['value'].strip()
                if not product_id:
                    el = soup.find(attrs={"data-product-id": True})
                    if el:
                        product_id = el['data-product-id'].strip()
                if not product_id:
                    meta_id = soup.find('meta', {'property': 'product:retailer_item_id'}) or soup.find('meta', {'name': 'product_id'})
                    if meta_id and meta_id.get('content'):
                        product_id = meta_id['content'].strip()

            if not name:
                title_el = soup.find('span', {'data-ui-id': 'page-title-wrapper'}) or soup.find('h1', class_='page-title') or soup.find('h1')
                if title_el:
                    name = title_el.get_text(strip=True)
                elif soup.title:
                    name = soup.title.get_text(strip=True).split('|')[0].split('-')[0].strip()

            if not sku and product_id:
                sku = product_id

            if not mpn:
                mpn = sku

            if not price:
                price_el = soup.find('span', class_='price') or soup.find('span', {'data-price-type': 'finalPrice'}) or soup.find('meta', {'property': 'product:price:amount'})
                if price_el:
                    if price_el.name == 'meta':
                        price = price_el.get('content', '').strip()
                    else:
                        price = re.sub(r'[^\d.]', '', price_el.get_text(strip=True))

            if not main_image:
                og_img = soup.find('meta', {'property': 'og:image'}) or soup.find('meta', {'name': 'twitter:image'})
                if og_img and og_img.get('content'):
                    main_image = og_img['content'].strip()

            if not brand:
                brand_meta = soup.find('meta', {'property': 'product:brand'}) or soup.find('meta', {'name': 'brand'})
                if brand_meta and brand_meta.get('content'):
                    brand = brand_meta['content'].strip()

        # ---------- Stock Status ----------
        availability = product_data.get('magentoProductAvailability', '')
        status = 'OUT_OF_STOCK'
        if availability == 'InStock':
            status = 'SELLABLE'
        elif raw_html and ('in stock' in raw_html.lower() or 'available' in raw_html.lower()):
            status = 'SELLABLE'

        variation_id = ''
        group_attr_1 = ''
        group_attr_2 = ''

        if not product_id and sku:
            product_id = sku

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
        log(f"Error extracting product data: {e}", "ERROR")
        return {}

def process_product_data(product_url: str, writer, seen: set, stats: dict):
    """Process a single product URL"""
    if product_url in seen:
        return
    seen.add(product_url)
    
    log(f"Processing product URL: {product_url}", "DEBUG")
    
    data = fetch_json(product_url)

    if not data:
        stats['errors'] += 1
        log(f"No data found for product {product_url}", "ERROR")
        return
    
    # Extract data from response
    product_info = extract_product_data(data)
    product_id = product_info.get('product_id')

    # If product_id is still empty, derive from URL slug so valid items are not lost
    if not product_id:
        slug = urlparse(product_url).path.rstrip('/').split('/')[-1].replace('.html', '')
        if slug:
            product_id = slug
            product_info['product_id'] = slug
            if not product_info.get('name'):
                product_info['name'] = slug.replace('-', ' ').title()
            if not product_info.get('sku'):
                product_info['sku'] = slug
            if not product_info.get('mpn'):
                product_info['mpn'] = slug

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
        _url_list_file = f"url_list_{SITE_NAME}_chunk_{SITEMAP_OFFSET}.txt"
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