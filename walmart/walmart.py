import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from pathlib import Path
import urllib3

try:
    from curl_cffi import requests as cc_requests
    HAS_CURL_CFFI = True
except ImportError:
    cc_requests = None
    HAS_CURL_CFFI = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class WalmartScraper:

    # ============================================================
    #  Furniture Whitelist Configuration (Always Furniture Only)
    # ============================================================

    FURNITURE_TOKENS: Set[str] = {
        # Bedroom
        "bed", "beds", "bedroom", "headboard", "headboards", "footboard", "footboards",
        "nightstand", "nightstands", "dresser", "dressers", "chest", "chests", "armoire",
        "armoires", "wardrobe", "wardrobes", "mattress", "mattresses", "futon", "futons",
        "daybed", "daybeds", "trundle", "crib", "cribs", "bassinet", "bassinets",
        # Living Room & Seating
        "sofa", "sofas", "couch", "couches", "loveseat", "loveseats", "sectional", "sectionals",
        "recliner", "recliners", "reclining", "ottoman", "ottomans", "pouf", "poufs",
        "chaise", "chaises", "armchair", "armchairs", "rocker", "glider",
        # Dining & Kitchen
        "sideboard", "sideboards", "buffet", "buffets", "credenza", "credenzas", "hutch", "hutches",
        "server", "servers", "barstool", "barstools", "bistro",
        # Tables, Storage & Office
        "table", "tables", "chair", "chairs", "bench", "benches", "stool", "stools",
        "bookcase", "bookcases", "bookshelf", "bookshelves", "cabinet", "cabinets",
        "desk", "desks", "vanity", "vanities", "furniture"
    }

    FURNITURE_PHRASES: List[str] = [
        "dining-table", "dining-chair", "dining-set", "kitchen-table", "coffee-table",
        "end-table", "side-table", "console-table", "accent-table", "accent-chair",
        "tv-stand", "media-console", "entertainment-center", "hall-tree", "shoe-rack",
        "coat-rack", "storage-bench", "bunk-bed", "loft-bed", "box-spring", "living-room",
        "dining-room", "home-office", "patio-furniture", "outdoor-furniture", "kids-furniture"
    ]

    FURNITURE_BRANDS: List[str] = [
        "ashley", "modway", "coaster", "sauder", "zinus", "dhp", "hillsdale",
        "walker-edison", "homestyles", "flash-furniture", "furniture-of-america",
        "steve-silver", "homelegance", "linon", "baxton-studio", "acme-furniture",
        "better-homes-gardens", "mainstays"
    ]

    NON_FURNITURE_CATEGORIES: List[str] = [
        "clothing", "shoes", "grocery", "food", "health", "beauty", "electronics",
        "cell phones", "computers", "video games", "toys", "baby clothing", "auto & tires",
        "automotive", "sports & outdoors", "lawn mowers", "movies", "books", "pharmacy"
    ]

    # ============================================================
    #  Init — all config + shared state
    # ============================================================

    def __init__(self):
        self.curr_url             = os.getenv("CURR_URL", "https://www.walmart.com").rstrip("/")
        self.sitemap_path         = os.getenv("SITEMAP_PATH", "sitemap_hi_ip.xml").strip()
        self.api_base_url         = os.getenv("API_BASE_URL", "").rstrip("/")
        self.sitemap_offset       = int(os.getenv("SITEMAP_OFFSET", "0"))
        self.max_sitemaps         = int(os.getenv("MAX_SITEMAPS", "0"))
        self.max_urls_per_sitemap = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
        self.max_workers          = int(os.getenv("MAX_WORKERS", "4"))
        self.request_delay        = float(os.getenv("REQUEST_DELAY", "1.0"))
        self.proxy                = (
            os.getenv("PROXY")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("HTTP_PROXY")
            or ""
        ).strip()

        self.output_dir   = "media/output/scrapping/walmart"
        self.failure_dir  = "media/output/scrapping/failure_csv"
        self.output_csv   = f"walmart_products_chunk_{self.sitemap_offset}.csv"
        self.scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        self.csv_header = [
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
            "Date Scrapped",
        ]

        self.csv_lock  = threading.Lock()
        self.fail_lock = threading.Lock()
        self.seen      = set()
        self.stats     = {
            "sitemaps_processed": 0,
            "urls_evaluated":     0,
            "furniture_urls":     0,
            "products_fetched":   0,
            "errors":             0,
        }

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language":           "en-US,en;q=0.9",
            "Accept-Encoding":           "gzip, deflate, br",
            "Connection":                "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua":                 '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile":          "?0",
            "Sec-Ch-Ua-Platform":        '"Windows"',
            "Sec-Fetch-Dest":            "document",
            "Sec-Fetch-Mode":            "navigate",
            "Sec-Fetch-Site":            "none",
            "Sec-Fetch-User":            "?1",
        })

    # ============================================================
    #  Logger
    # ============================================================

    def log(self, msg: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
        sys.stderr.flush()

    # ============================================================
    #  Furniture URL & Category Filters
    # ============================================================

    def is_furniture_url(self, url: str) -> bool:
        """
        Pre-filter: check if Walmart /ip/ URL slug matches furniture keywords or brands.
        Avoids making HTTP requests for non-furniture URLs.
        """
        if not url or "/ip/" not in url:
            return False
        match = re.search(r'/ip/([^/]+)/', url)
        if not match:
            return False
        slug = match.group(1).lower()

        # Check token match (e.g. bed, sofa, dresser, desk)
        tokens = set(re.split(r'[-_]', slug))
        if tokens & self.FURNITURE_TOKENS:
            return True

        # Check multi-word phrase match (e.g. dining-table, tv-stand)
        for phrase in self.FURNITURE_PHRASES:
            if phrase in slug:
                return True

        # Check known furniture brand in slug
        for brand in self.FURNITURE_BRANDS:
            if brand in slug:
                return True

        return False

    def is_furniture_category(self, category_path: str) -> bool:
        """
        Post-filter: verify the product category belongs to Furniture or Home/Mattress.
        """
        if not category_path:
            # If category could not be parsed, trust the URL pre-filter
            return True
        cat_lower = category_path.lower()
        if "furniture" in cat_lower or "mattress" in cat_lower or "bedroom" in cat_lower or "seating" in cat_lower:
            return True
        for non_f in self.NON_FURNITURE_CATEGORIES:
            if non_f in cat_lower:
                return False
        return True

    # ============================================================
    #  HTTP
    # ============================================================

    @staticmethod
    def is_bot_challenge(text: str) -> bool:
        """Detect Walmart PerimeterX / bot challenge HTML pages."""
        if not text:
            return False
        head = text[:4000]
        return (
            "Robot or human?" in head
            or 'id="blockScript"' in head
            or "blocked.walmart.com" in head
            or "/px/captcha" in head
        )

    def http_get(self, url: str, is_json: bool = False) -> Optional[str]:
        """GET with up to 3 retries using curl_cffi TLS impersonation (fallback to requests)."""
        impersonations = ["chrome124", "chrome120", "chrome110"]
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None

        for attempt in range(3):
            try:
                if is_json:
                    headers = {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                        "Accept":           "application/json, text/javascript, */*; q=0.01",
                        "Accept-Language":  "en-US,en;q=0.9",
                        "Referer":          f"{self.curr_url}/",
                        "X-Requested-With": "XMLHttpRequest",
                        "Sec-Fetch-Dest":   "empty",
                        "Sec-Fetch-Mode":   "cors",
                        "Sec-Fetch-Site":   "same-origin",
                    }
                else:
                    headers = dict(self.session.headers)

                if HAS_CURL_CFFI:
                    imp = impersonations[attempt % len(impersonations)]
                    r = cc_requests.get(
                        url,
                        headers=headers,
                        impersonate=imp,
                        proxies=proxies,
                        timeout=20,
                        verify=True,
                    )
                else:
                    r = self.session.get(
                        url,
                        headers=headers if is_json else None,
                        proxies=proxies,
                        timeout=20,
                        verify=True,
                    )

                if r.status_code == 200:
                    if self.is_bot_challenge(r.text):
                        self.log(
                            f"Bot challenge ('Robot or human?') received for {url} "
                            f"(attempt {attempt + 1}/3)",
                            "WARNING",
                        )
                        time.sleep(3 * (attempt + 1))
                        continue
                    self.log(f"Success: {url}", "DEBUG")
                    return r.text

                self.log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code in (429, 403, 412):
                    time.sleep(5 * (attempt + 1))

            except Exception as e:
                if "Timeout" in type(e).__name__:
                    self.log(f"Timeout attempt {attempt + 1} for {url}", "WARNING")
                    time.sleep(2)
                else:
                    self.log(f"Attempt {attempt + 1} failed for {url}: {type(e).__name__}", "WARNING")
                    time.sleep(1)

        return None

    # ============================================================
    #  Sitemap
    # ============================================================

    def load_xml(self, url: str) -> Optional[ET.Element]:
        """Fetch and parse an XML sitemap. Strips <script/> before parsing."""
        data = self.http_get(url, is_json=False)

        if not data:
            self.log(f"Failed to load XML from {url}", "ERROR")
            return None

        stripped = data.lstrip()
        if stripped.startswith("<!DOCTYPE html") or stripped.startswith("<html"):
            self.log(
                f"Expected XML from {url} but received HTML (likely bot block / geo block). "
                f"Configure PROXY secret if running from a datacenter IP.",
                "ERROR",
            )
            return None

        # Walmart injects <script/> into the sitemap index which breaks XML parsing
        data = re.sub(r'<script[^>]*/>', '', data)
        data = re.sub(r'<script[^>]*>.*?</script>', '', data, flags=re.DOTALL)
        try:
            if "<?xml" not in data[:100]:
                data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data
            return ET.fromstring(data)
        except ET.ParseError as e:
            self.log(f"XML parse error for {url}: {e}", "ERROR")
            return None

    def get_child_sitemaps(self, index_url: str) -> List[str]:
        """Parse sitemap index and return list of child sitemap URLs."""
        ns    = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        index = self.load_xml(index_url)
        if index is None:
            return []

        for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
            elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
            if elements:
                sitemaps = [e.text.strip() for e in elements if e.text]
                if sitemaps:
                    self.log(f"Found {len(sitemaps)} child sitemaps in index", "INFO")
                    return sitemaps

        # If index_url was already a direct sitemap rather than an index
        self.log("No child sitemaps in index; treating as direct sitemap URL", "INFO")
        return [index_url]

    def get_product_urls(self, sitemap_url: str) -> List[str]:
        """Parse a product sitemap and return ONLY Walmart furniture /ip/ URLs."""
        ns  = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        xml = self.load_xml(sitemap_url)
        if not xml:
            return []

        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
            if elements:
                all_urls = [e.text.strip() for e in elements if e.text and "/ip/" in e.text]
                self.stats["urls_evaluated"] += len(all_urls)
                # Apply FURNITURE ONLY filter
                furniture_urls = [u for u in all_urls if self.is_furniture_url(u)]
                self.stats["furniture_urls"] += len(furniture_urls)
                self.log(
                    f"Sitemap: {len(all_urls)} total URLs -> {len(furniture_urls)} "
                    f"furniture-matched URLs ({len(furniture_urls)/len(all_urls)*100:.1f}%)"
                    if all_urls else f"No URLs found in {sitemap_url}"
                )
                return furniture_urls

        self.log(f"No /ip/ product URLs found in: {sitemap_url}", "WARNING")
        return []

    # ============================================================
    #  Helpers
    # ============================================================

    def extract_product_id(self, url: str) -> Optional[str]:
        """Pull Walmart item ID from /ip/product-name/123456789 style URLs."""
        if not url:
            return None
        match = re.search(r'/ip/(?:[^/]+/)?(\d+)', url)
        if match:
            return match.group(1)
        last = url.rstrip("/").split("/")[-1].split("?")[0]
        if last.isdigit():
            return last
        self.log(f"No product ID found in URL: {url}", "WARNING")
        return None

    def clean_url(self, url: str) -> str:
        """Strip query params and trailing slashes."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"

    def normalize_image(self, url: str) -> str:
        """Make image URL absolute."""
        if not url:
            return ""
        if url.startswith("//"):
            return "https:" + url
        if url.startswith("/"):
            return f"{self.curr_url}{url}"
        if not url.startswith("http"):
            return f"https://{url}"
        return url

    # ============================================================
    #  Product Extraction (JSON-LD + __NEXT_DATA__)
    # ============================================================

    def extract_walmart_data(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """
        Parse JSON-LD and __NEXT_DATA__ blocks from a Walmart page.
        Returns one dict per variant, filtered strictly to furniture.
        """
        product_id = self.extract_product_id(url)
        results: List[Dict] = []
        detected_category = ""

        # Extract Category Breadcrumbs if available
        breadcrumb_elements = soup.select('nav[aria-label="breadcrumb"] li, ol[itemtype*="BreadcrumbList"] li')
        if breadcrumb_elements:
            crumbs = [b.get_text(strip=True) for b in breadcrumb_elements if b.get_text(strip=True)]
            detected_category = " / ".join(crumbs)

        # ------------------------------------------------------------
        # 1. Primary: JSON-LD extraction
        # ------------------------------------------------------------
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string
                if not raw:
                    continue
                data = json.loads(raw)

                if isinstance(data, list):
                    data = data[0]
                if not isinstance(data, dict):
                    continue

                # Check breadcrumb in JSON-LD if present
                if data.get("@type") == "BreadcrumbList" and "itemListElement" in data:
                    crumbs = [item.get("name", "") for item in data.get("itemListElement", []) if isinstance(item, dict)]
                    if crumbs:
                        detected_category = " / ".join(crumbs)
                    continue

                # Skip non-product JSON-LD schemas (e.g., WebPage, Organization)
                schema_type = data.get("@type")
                is_product_schema = (
                    schema_type in ("Product", "ProductGroup")
                    or (isinstance(schema_type, list) and any(t in ("Product", "ProductGroup") for t in schema_type))
                    or "hasVariant" in data
                )
                if not is_product_schema:
                    continue

                variants = data.get("hasVariant") or [data]

                for variant in variants:
                    if not isinstance(variant, dict):
                        continue

                    # Pick best matching offer
                    offers         = variant.get("offers", {})
                    selected_offer = {}
                    if isinstance(offers, list):
                        for offer in offers:
                            offer_url = self.clean_url(offer.get("url", ""))
                            if self.extract_product_id(offer_url) == product_id:
                                selected_offer = offer
                                break
                        if not selected_offer and offers:
                            selected_offer = offers[0]
                    elif isinstance(offers, dict):
                        selected_offer = offers

                    images     = variant.get("image", "")
                    main_image = images[0] if isinstance(images, list) else images or ""

                    price = (
                        selected_offer.get("price", "")
                        or selected_offer.get("lowPrice", "")
                        or (data.get("offers", {}).get("price", "")
                            if isinstance(data.get("offers"), dict) else "")
                    )

                    availability = selected_offer.get("availability", "") or variant.get("availability", "")
                    status       = "In Stock" if "InStock" in availability else "Out of Stock"

                    brand_raw = variant.get("brand", {})
                    brand     = brand_raw.get("name", "") if isinstance(brand_raw, dict) else str(brand_raw)
                    name      = variant.get("name", "") or data.get("name", "")
                    if not name:
                        continue

                    results.append({
                        "competitor_product_id": product_id or "",
                        "comp_received_name":    name,
                        "comp_received_sku":     variant.get("sku", ""),
                        "brand":                 brand,
                        "mpn":                   variant.get("model", "") or variant.get("mpn", ""),
                        "category":              detected_category or "Furniture",
                        "category_url":          "",
                        "gtin":                  variant.get("gtin13", "") or variant.get("gtin", ""),
                        "variation_id":          variant.get("variationId", ""),
                        "quantity":              variant.get("inventory", {}).get("quantityAvailable", 0),
                        "status":                status,
                        "competitor_price":      price,
                        "group_attr_1":          variant.get("description", ""),
                        "group_attr_2":          variant.get("color", ""),
                        "main_image":            self.normalize_image(main_image),
                        "competitor_url":        url,
                        "scraped_date":          self.scraped_date,
                    })

                if results:
                    break

            except (json.JSONDecodeError, AttributeError) as e:
                self.log(f"JSON-LD parse error: {e}", "WARNING")
                continue

        # ------------------------------------------------------------
        # 2. Fallback: __NEXT_DATA__ extraction (Modern Walmart Pages)
        # ------------------------------------------------------------
        if not results:
            next_script = soup.find("script", id="__NEXT_DATA__")
            if next_script and next_script.string:
                try:
                    next_json = json.loads(next_script.string)
                    prod = (
                        next_json.get("props", {})
                        .get("pageProps", {})
                        .get("initialData", {})
                        .get("data", {})
                        .get("product", {})
                    )
                    if prod:
                        p_name = prod.get("name", "")
                        p_brand = prod.get("brand", "")
                        p_sku = prod.get("sku") or prod.get("usItemId") or ""
                        p_model = prod.get("model") or ""
                        p_upc = prod.get("upc") or prod.get("gtin13") or ""
                        p_price = (prod.get("priceInfo") or {}).get("currentPrice", {}).get("price", "")
                        p_status = "In Stock" if prod.get("availabilityStatus") == "IN_STOCK" else "Out of Stock"
                        img_info = prod.get("imageInfo") or {}
                        p_image = img_info.get("thumbnailUrl", "")
                        if not p_image and isinstance(img_info.get("allImages"), list) and img_info["allImages"]:
                            p_image = img_info["allImages"][0].get("url", "")

                        cat_obj = prod.get("category") or {}
                        cat_path_list = cat_obj.get("path", []) if isinstance(cat_obj, dict) else []
                        if cat_path_list:
                            p_cat = " / ".join(c.get("name", "") for c in cat_path_list if c.get("name"))
                            last_cat_url = cat_path_list[-1].get("url", "")
                            p_cat_url = f"{self.curr_url}{last_cat_url}" if last_cat_url.startswith("/") else last_cat_url
                        else:
                            p_cat = prod.get("categoryPath") or detected_category or "Furniture"
                            p_cat_url = ""

                        results.append({
                            "competitor_product_id": product_id or str(prod.get("usItemId", "")),
                            "comp_received_name":    p_name,
                            "comp_received_sku":     str(p_sku) if p_sku else "",
                            "brand":                 p_brand,
                            "mpn":                   p_model,
                            "category":              p_cat,
                            "category_url":          p_cat_url,
                            "gtin":                  p_upc,
                            "variation_id":          "",
                            "quantity":              0,
                            "status":                p_status,
                            "competitor_price":      p_price,
                            "group_attr_1":          prod.get("shortDescription", ""),
                            "group_attr_2":          "",
                            "main_image":            self.normalize_image(p_image),
                            "competitor_url":        url,
                            "scraped_date":          self.scraped_date,
                        })
                except Exception as e:
                    self.log(f"__NEXT_DATA__ parse error: {e}", "WARNING")

        # ------------------------------------------------------------
        # 3. Post-Filter: Verify category belongs to Furniture
        # ------------------------------------------------------------
        furniture_results = [
            r for r in results
            if self.is_furniture_category(r.get("category", ""))
        ]
        return furniture_results

    # ============================================================
    #  CSV Write
    # ============================================================

    def write_row(self, writer: csv.writer, product: Dict):
        """Thread-safe write of one product row to CSV."""
        row = [
            product["competitor_url"],
            product["competitor_product_id"],
            product["variation_id"],
            product["category"],
            product["category_url"],
            product["brand"],
            product["comp_received_name"],
            product["comp_received_sku"],
            product["mpn"],
            product["gtin"],
            product["competitor_price"],
            product["main_image"],
            product["quantity"],
            product["group_attr_1"],
            product["group_attr_2"],
            product["status"],
            product["scraped_date"],
        ]
        with self.csv_lock:
            writer.writerow(row)

    def log_failure(self, url: str, reason: str):
        """Append a failed URL to the failure CSV."""
        os.makedirs(self.failure_dir, exist_ok=True)
        failure_path = os.path.join(self.failure_dir, "Walmart_failures.csv")
        file_exists  = os.path.isfile(failure_path)
        with self.fail_lock:
            with open(failure_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["URL", "Reason", "Timestamp"])
                w.writerow([url, reason, self.scraped_date])

    # ============================================================
    #  Process Single Product
    # ============================================================

    def process_product(self, product_url: str, writer: csv.writer):
        """Fetch, extract, and save one Walmart product URL."""
        base_url = self.clean_url(product_url)

        if base_url in self.seen:
            return
        self.seen.add(base_url)

        self.log(f"Processing: {base_url}", "DEBUG")

        product_id = self.extract_product_id(base_url)
        if not product_id:
            self.stats["errors"] += 1
            self.log_failure(base_url, "No product ID extracted")
            return

        html = self.http_get(base_url, is_json=False)
        if not html:
            self.stats["errors"] += 1
            self.log_failure(base_url, "HTTP fetch failed")
            return

        soup     = BeautifulSoup(html, "html.parser")
        products = self.extract_walmart_data(soup, base_url)

        if not products:
            self.stats["errors"] += 1
            self.log_failure(base_url, "No furniture data extracted or not in furniture category")
            return

        for product in products:
            if not product.get("comp_received_name"):
                continue
            try:
                self.write_row(writer, product)
                self.stats["products_fetched"] += 1
                self.log(
                    f"Saved [{product['competitor_product_id']}] "
                    f"{product['comp_received_name'][:60]}"
                )
            except Exception as e:
                self.log(f"Row write error for {product_id}: {e}", "ERROR")
                self.stats["errors"] += 1

        time.sleep(self.request_delay)

    # ============================================================
    #  Run — main orchestrator
    # ============================================================

    def run(self):
        self.log("=" * 60)
        self.log("Walmart Furniture Scraper (Always Furniture Only)")
        self.log(f"Timestamp:            {self.scraped_date}")
        self.log(f"Base URL:             {self.curr_url}")
        self.log(f"Sitemap:              {self.sitemap_path}")
        self.log(f"Sitemap Offset:       {self.sitemap_offset}")
        self.log(f"Max Sitemaps:         {self.max_sitemaps or 'All'}")
        self.log(f"Max URLs per Sitemap: {self.max_urls_per_sitemap or 'All'}")
        self.log(f"Max Workers:          {self.max_workers}")
        self.log(f"Request Delay:        {self.request_delay}s")
        self.log("=" * 60)

        # Resolve Sitemap URL (supports relative path or full URL)
        if self.sitemap_path.startswith("http"):
            sitemap_index_url = self.sitemap_path
        else:
            sitemap_index_url = f"{self.curr_url}/{self.sitemap_path.lstrip('/')}"

        all_sitemaps = self.get_child_sitemaps(sitemap_index_url)

        if not all_sitemaps:
            self.log("No sitemaps found. Exiting.", "ERROR")
            sys.exit(1)

        if self.sitemap_offset >= len(all_sitemaps):
            self.log(f"Offset {self.sitemap_offset} >= total ({len(all_sitemaps)}), nothing to do.", "WARNING")
            sys.exit(0)

        end_idx         = self.sitemap_offset + self.max_sitemaps if self.max_sitemaps > 0 else len(all_sitemaps)
        sitemaps_to_run = all_sitemaps[self.sitemap_offset:end_idx]

        self.log(f"Total sitemaps found:  {len(all_sitemaps)}")
        self.log(f"Sitemaps to process:   {len(sitemaps_to_run)}")

        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, self.output_csv)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(self.csv_header)

            self.all_product_urls: list = []  # track URLs for FTP tracking

            for sitemap_url in sitemaps_to_run:
                self.stats["sitemaps_processed"] += 1
                self.log(f"Sitemap {self.stats['sitemaps_processed']}/{len(sitemaps_to_run)}: {sitemap_url}")

                urls = self.get_product_urls(sitemap_url)
                if not urls:
                    continue
                self.all_product_urls.extend(urls)  # collect for URL tracking

                if self.max_urls_per_sitemap > 0:
                    self.log(f"Limiting to {self.max_urls_per_sitemap} of {len(urls)} furniture URLs")
                    urls = urls[:self.max_urls_per_sitemap]
                else:
                    self.log(f"Processing all {len(urls)} furniture URLs")

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self.process_product, url, writer)
                        for url in urls
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            self.log(f"Thread error: {e}", "ERROR")
                            self.stats["errors"] += 1

                gc.collect()

        self.log("=" * 60)
        self.log("SCRAPING COMPLETE (FURNITURE ONLY)")
        self.log(f"  Sitemaps processed:  {self.stats['sitemaps_processed']}")
        self.log(f"  Total URLs evaluated: {self.stats['urls_evaluated']}")
        self.log(f"  Furniture URLs found: {self.stats['furniture_urls']}")
        self.log(f"  Products saved:      {self.stats['products_fetched']}")
        self.log(f"  Errors:              {self.stats['errors']}")
        if self.stats["furniture_urls"] > 0:
            rate = self.stats["products_fetched"] / self.stats["furniture_urls"] * 100
            self.log(f"  Success rate:        {rate:.1f}%")
        self.log(f"  Output:              {output_path}")
        self.log("=" * 60)


# ============================================================
#  Entry point
# ============================================================

if __name__ == "__main__":
    if not os.getenv("CURR_URL"):
        sys.stderr.write("[ERROR] CURR_URL environment variable is required\n")
        sys.exit(1)

    _walmart_scraper = WalmartScraper()
    _walmart_scraper.run()

    # Save per-chunk URL list for artifact upload (merged in YML merge job)
    if hasattr(_walmart_scraper, 'all_product_urls') and _walmart_scraper.all_product_urls:
        try:
            _url_f = f"url_list_chunk_{_walmart_scraper.sitemap_offset}.txt"
            with open(_url_f, "w", encoding="utf-8") as _uf:
                _uf.write("\n".join(_walmart_scraper.all_product_urls))
            _walmart_scraper.log(f"[URL-LIST] Saved {len(_walmart_scraper.all_product_urls)} furniture URLs to {_url_f}")
        except Exception as _exc:
            import sys as _sys; _sys.stderr.write(f"[URL-LIST] Warning: {_exc}\n")
