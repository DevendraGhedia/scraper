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
from urllib.parse import urlparse

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

CURR_URL = os.getenv("CURR_URL", "https://www.emmamason.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
SITEMAP_URL = os.getenv("SITEMAP_URL", "").strip()
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "5"))
PROXY_ENV = os.getenv("PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("SITEMAP_PROXY") or ""

# FlareSolverr configuration
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1").rstrip("/")
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "120"))

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
            "Referer": CURR_URL + "/",
        }
        self.session.headers.update(self.browser_headers)

    def _get_proxies_dict(self):
        p = self.proxy
        if not p and self.proxy_manager:
            try:
                if hasattr(self.proxy_manager, 'get_proxy'):
                    p = self.proxy_manager.get_proxy(CURR_URL)
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

    # 2. Try robots.txt
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Attempting to fetch robots.txt from {robots_url}")
    robots_text = bypass_session.fetch(robots_url, timeout=20)
    if robots_text:
        match = re.search(r'Sitemap:\s*(https?://[^\s\r\n<]+)', robots_text, re.IGNORECASE)
        if match:
            sitemap_val = match.group(1).strip()
            log(f"Extracted Sitemap URL from robots.txt: {sitemap_val}")
            return sitemap_val

    log("Could not extract sitemap from robots.txt, checking common paths...", "WARNING")

    # 3. Fallback common paths
    candidates = [
        f"{CURR_URL}/sitemap.xml",
        f"{CURR_URL}/sitemaps/sitemap.xml",
        f"{CURR_URL}/sitemap_index.xml",
        f"{CURR_URL}/sitemap/sitemap.xml",
        f"{CURR_URL}/pub/media/sitemap.xml",
        f"{CURR_URL}/pub/media/sitemap/sitemap.xml",
    ]
    for candidate in candidates:
        log(f"Probing candidate sitemap path: {candidate}", "DEBUG")
        content = bypass_session.fetch(candidate, timeout=20)
        if content and ("<loc>" in content or "<urlset" in content or "<sitemapindex" in content):
            log(f"Found active sitemap candidate: {candidate}")
            return candidate

    # Default to SITEMAP_INDEX if all checks fail
    return SITEMAP_INDEX

def check_robots_txt() -> Tuple[Optional[float], Optional[str]]:
    """Check robots.txt for crawl-delay and sitemap directive"""
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    content = bypass_session.fetch(robots_url, timeout=20)
    crawl_delay = None
    sitemap_url = None
    if content:
        s_match = re.search(r'Sitemap:\s*(https?://[^\s\r\n<]+)', content, re.IGNORECASE)
        if s_match:
            sitemap_url = s_match.group(1).strip()
            log(f"Found valid sitemap in robots.txt: {sitemap_url}")
        cd_match = re.search(r'crawl-delay:\s*(\d+(?:\.\d+)?)', content, re.IGNORECASE)
        if cd_match:
            try:
                crawl_delay = float(cd_match.group(1))
                log(f"Found Crawl-delay: {crawl_delay} seconds")
            except ValueError:
                pass
    return crawl_delay, sitemap_url

# ================= XML & DATA LAYER PARSING =================

def load_xml(url: str, crawl_delay=None) -> Optional[ET.Element]:
    """
    Load XML sitemap with unwrap handling for Chrome XML viewer HTML wrapper and
    regex fallback if XML parser encounters malformed or mismatched tags.
    """
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

        # Remove default XML namespace to make queries simple and direct
        data_clean = re.sub(r'\sxmlns="[^"]+"', '', data, count=1)

        # Clean XML declaration if needed
        if "<?xml" not in data_clean[:100] and not data_clean.strip().startswith("<"):
            data_clean = '<?xml version="1.0" encoding="UTF-8"?>\n' + data_clean

        return ET.fromstring(data_clean)
    except ET.ParseError as e:
        log(f"XML parsing failed for {url}: {e}, attempting regex extraction", "WARNING")
        # Try to extract URLs with regex fallback
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

def _clean_strings(obj):
    """Recursively clean JSON-escaped strings like \\/"""
    if isinstance(obj, dict):
        return {k: _clean_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_strings(v) for v in obj]
    if isinstance(obj, str):
        return obj.replace('\\/', '/')
    return obj

def extract_datalayer(html_text):
    patterns = [
        r'dataLayer\.push\s*\(\s*(\{[\s\S]*?\})\s*\);',
    ]
    raw = None
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            raw = match.group(1)
            break
    if not raw:
        return None

    raw = html.unescape(raw)
    raw = raw.replace(':true', ':True') \
             .replace(':false', ':False') \
             .replace(':null', ':None') \
             .replace('true,', 'True,') \
             .replace('false,', 'False,') \
             .replace('null,', 'None,')

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            if raw.strip().startswith('{'):
                raw = f'[{raw}]'
            data = ast.literal_eval(raw)
        except (SyntaxError, ValueError):
            raw = re.sub(r',\s*}', '}', raw)
            raw = re.sub(r',\s*]', ']', raw)
            try:
                data = json.loads(raw)
            except Exception:
                return None

    return _clean_strings(data)

def extract_additional_product_info(html_text):
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        table = soup.find('table', id='product-attribute-specs-table')
        if not table:
            table = soup.find('table', class_='additional-attributes')
            if not table:
                return json.dumps({})

        additional_info = {}
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            if th and td:
                label_text = th.get_text(strip=True)
                data_text = td.get_text(strip=True)
                if label_text and data_text:
                    json_key = re.sub(r'[^a-zA-Z0-9_]', '_', label_text.lower().replace(' ', '_'))
                    json_key = json_key.strip('_')
                    additional_info[json_key] = data_text

        return json.dumps(additional_info, ensure_ascii=False)
    except Exception as e:
        log(f"Error while processing additional Data: {e}", "DEBUG")
        return json.dumps({})

def fetch_json(url: str, crawl_delay=None, check_is_pdp_only: bool = False) -> Optional[dict]:
    """Fetch product page HTML using bypass engine and extract dataLayer + additional product info"""
    data = bypass_session.fetch(url, is_json=False, timeout=25)
    if not data:
        return None
    try:
        data_layer = extract_datalayer(data)
        if not data_layer:
            return None

        product_data = data_layer[0] if isinstance(data_layer, list) else data_layer
        is_pdp = product_data.get("ecommerce", {}).get("isPDP", None)

        if check_is_pdp_only:
            return {"isPDP": is_pdp}

        if is_pdp == 0:
            return None

        additional_info = extract_additional_product_info(data)
        product_data["additional_product_info_html"] = additional_info
        product_data["_raw_html"] = data
        return product_data
    except json.JSONDecodeError as e:
        log(f"JSON decode error for {url}: {e}", "DEBUG")
        return None
    except Exception as e:
        log(f"Error processing data for {url}: {e}", "DEBUG")
        return None

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CURR_URL}{url}"
    elif not url.startswith("http"):
        return f"https://{url}"
    return url

def extract_product_data(product_data: dict) -> dict:
    try:
        # Product ID
        product_id = ''
        if isinstance(product_data.get('ecomm_prodid'), list) and product_data.get('ecomm_prodid'):
            product_id = str(product_data.get('ecomm_prodid')[0])
        elif product_data.get('ecomm_prodid'):
            product_id = str(product_data.get('ecomm_prodid'))

        if not product_id:
            product_id = str(product_data.get('product', {}).get('id', '')).strip()
        if not product_id:
            product_id = str(product_data.get('magentoProductId', '')).strip()

        # Product Name
        ecommerce_items = product_data.get('ecommerce', {}).get('items', [])
        name = ''
        if ecommerce_items:
            name = ecommerce_items[0].get('item_name', '').strip()
        if not name:
            name = product_data.get('product', {}).get('name', '').strip()
        if not name:
            name = product_data.get('magentoProductName', '').strip()

        # SKU
        sku = product_data.get('ecomm_prodsku', '')
        if not sku:
            sku = product_data.get('product', {}).get('sku', '')
        if not sku:
            sku = product_data.get('magentoProductSku', '')

        # Brand & Quantity
        brand = ''
        quantity = 0
        if ecommerce_items:
            brand = ecommerce_items[0].get('item_brand', '')
            quantity = ecommerce_items[0].get('quantity', 0)
        if not brand:
            brand = product_data.get('product', {}).get('brand', '')

        # Price
        price = ''
        if ecommerce_items:
            price_item = ecommerce_items[0].get('price', '')
            if price_item:
                price = str(price_item)
        if not price:
            ecomm_value = product_data.get('ecommerce', {}).get('value', '')
            if ecomm_value:
                price = str(ecomm_value)
        if not price:
            price = str(product_data.get('magentoProductPrice', ''))

        # Main Image
        main_image = product_data.get('magentoProductImage1', '')
        if not main_image:
            main_image = product_data.get('product', {}).get('image', '')

        # Additional data & MPN
        additional_data = product_data.get('additional_product_info_html', '')
        raw_html = product_data.get('_raw_html', '')
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

        # Category
        category_url = ''
        if ecommerce_items and not category:
            category_fields = [
                'item_category', 'item_category2', 'item_category3',
                'item_category4', 'item_category5', 'item_category6',
                'item_category7', 'item_category8', 'item_category9'
            ]
            categories = []
            for field in category_fields:
                cat_value = ecommerce_items[0].get(field, '')
                if cat_value:
                    categories.append(cat_value)
            if categories:
                category = ' | '.join(categories)

        # Stock Status
        availability = product_data.get('ecommerce', {}).get('magentoProductAvailability', '')
        if not availability:
            availability = product_data.get('magentoProductAvailability', '')
        status = 'OUT_OF_STOCK'
        if availability == 'InStock':
            status = 'SELLABLE'

        variation_id = ''
        group_attr_1 = ''
        group_attr_2 = ''

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

def process_product_data(product_url: str, writer, seen: set, stats: dict, crawl_delay=None):
    if product_url in seen:
        return
    seen.add(product_url)

    log(f"Processing product URL: {product_url}", "DEBUG")
    data = fetch_json(product_url, crawl_delay)

    if not data:
        stats['errors'] += 1
        log(f"No data found for product {product_url}", "WARNING")
        return

    product_info = extract_product_data(data)
    if not product_info.get('product_id'):
        stats['errors'] += 1
        log(f"Invalid data for product {product_url}", "WARNING")
        return

    try:
        row = [
            product_url,
            product_info['product_id'],
            product_info['variation_id'],
            product_info['category'],
            product_info['category_url'],
            product_info['brand'],
            product_info['name'],
            product_info['sku'],
            product_info['mpn'],
            '',
            product_info['price'],
            normalize_image_url(product_info['main_image']),
            product_info['quantity'],
            product_info['group_attr_1'],
            product_info['group_attr_2'],
            product_info['status'],
            product_info['additional_data'],
            SCRAPED_DATE
        ]

        with csv_lock:
            writer.writerow(row)

        stats['products_fetched'] += 1
        log(f"Fetched product {product_info['product_id']}: {product_info['name'][:50]}...", "INFO")

    except Exception as e:
        log(f"Error creating row for product {product_info.get('product_id', 'unknown')}: {e}", "ERROR")
        stats['errors'] += 1

    time.sleep(REQUEST_DELAY_BASE)
    stats['urls_processed'] += 1

# ================= MAIN =================

def main():
    crawl_delay, robots_sitemap = check_robots_txt()
    sitemap = get_sitemap_url()
    if robots_sitemap and not SITEMAP_URL:
        sitemap = robots_sitemap
        log(f"Using sitemap from robots.txt: {sitemap}")
    else:
        log(f"Using sitemap: {sitemap}")

    if crawl_delay:
        if crawl_delay > 30:
            log(f"Crawl-delay {crawl_delay}s is too high, capping at 30s")
            crawl_delay = 30
        log(f"Respecting crawl-delay: {crawl_delay} seconds between requests")
    else:
        log(f"Using default request delay: {REQUEST_DELAY_BASE} seconds")

    log("=" * 60)
    log("Emma Mason Parallel Scraper")
    log(f"FlareSolverr URL: {FLARESOLVERR_URL or 'None'}")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"Sitemap Index: {sitemap}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY_BASE}s")
    log(f"curl_cffi Available: {HAS_CURL_CFFI}")
    log(f"cloudscraper Available: {HAS_CLOUDSCRAPER}")
    log(f"Proxy Configured: {'Yes' if PROXY_ENV else 'No'}")
    log("=" * 60)

    log(f"Loading sitemap index from {sitemap}")
    index = load_xml(sitemap, crawl_delay)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []

    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text and e.text.strip().endswith(".xml")]
            if sitemaps:
                break

    # If no .xml child sitemaps found, check if sitemap is directly a list of URLs
    if not sitemaps:
        all_locs = [e.text.strip() for e in index.findall(".//loc") if e.text]
        xml_locs = [u for u in all_locs if u.endswith(".xml")]
        if xml_locs:
            sitemaps = xml_locs
        elif all_locs:
            log("Sitemap appears to be a direct URL set rather than an index of sitemaps")
            sitemaps = [sitemap]
        else:
            sitemaps = [sitemap]

    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)

    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]

    log(f"Total sitemaps found: {len(sitemaps)}")
    log(f"Sitemaps to process: {len(sitemaps_to_process)}")

    all_product_urls: list = []

    # Initialize CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

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

        seen = set()
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

            xml = load_xml(sitemap_url, crawl_delay)
            if not xml:
                log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                continue

            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text
                        and not any(ext in e.text.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.pdf', '.css', '.js'])
                        and not e.text.strip().endswith('.xml')
                    ]
                    # If html URLs exist, prefer them
                    html_urls = [u for u in urls if '.html' in u]
                    if html_urls:
                        urls = html_urls
                    if urls:
                        all_product_urls.extend(urls)
                        break

            if not urls:
                log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                continue

            if MAX_URLS_PER_SITEMAP > 0:
                original_count = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Limited to {len(urls)} out of {original_count} URLs")
            else:
                log(f"Found {len(urls)} product URLs in this sitemap")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats, crawl_delay)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        stats['errors'] += 1

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
        url_list_file = f"url_list_chunk_{SITEMAP_OFFSET}.txt"
        try:
            with open(url_list_file, "w", encoding="utf-8") as uf:
                uf.write("\n".join(all_product_urls))
            log(f"[URL-LIST] Saved {len(all_product_urls)} URLs to {url_list_file}")
        except Exception as exc:
            log(f"[URL-LIST] Warning: {exc}", "WARNING")

if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)

    main()