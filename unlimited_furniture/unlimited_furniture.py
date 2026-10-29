import os
import csv
import time
import sys
import gc
import threading
import re
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from pathlib import Path
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from curl_cffi import requests
    HAS_CURL_CFFI = True
except ImportError:
    import requests
    HAS_CURL_CFFI = False


class unlimitedFurnitureScraper:

    # ============================================================
    #  Init — all config + shared state
    # ============================================================

    def __init__(self):
        self.curr_url             = os.getenv("CURR_URL", "https://www.unlimitedfurnituregroup.com").rstrip("/")
        self.api_base_url         = os.getenv("API_BASE_URL", "").rstrip("/")
        self.sitemap_url          = os.getenv("SITEMAP_URL", "").strip()
        self.sitemap_offset       = int(os.getenv("SITEMAP_OFFSET", "0"))
        self.max_sitemaps         = int(os.getenv("MAX_SITEMAPS", "0"))
        self.max_urls_per_sitemap = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
        self.max_workers          = int(os.getenv("MAX_WORKERS", "4"))
        self.request_delay        = float(os.getenv("REQUEST_DELAY", "0.5"))

        self.output_dir   = "media/output/scrapping/unlimitedfurniture"
        self.failure_dir  = "media/output/scrapping/failure_csv"
        self.output_csv   = f"unlimitedfurniture_products_chunk_{self.sitemap_offset}.csv"
        self.skipped_plp_csv = f"unlimitedfurniture_skipped_plp_{self.sitemap_offset}.csv"
        self.scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        self.csv_header = [
            "Ref Product URL",
            "Ref Product ID",
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
        self.all_product_urls: List[str] = []
        self.stats     = {
            "sitemaps_processed": 0,
            "urls_processed":     0,
            "products_fetched":   0,
            "errors":             0,
            "plp_urls_skipped":   0,
        }

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        })

    # ============================================================
    #  Logger
    # ============================================================

    def log(self, msg: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
        sys.stderr.flush()

    # ============================================================
    #  HTTP
    # ============================================================

    def http_get(self, url: str, is_json: bool = False) -> Optional[str]:
        """GET with up to 3 retries. Supports curl_cffi impersonation or standard requests."""
        for attempt in range(3):
            try:
                headers = {
                    "Accept": "application/json" if is_json else "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                }
                kwargs: Dict[str, Any] = {
                    "headers": headers,
                    "timeout": 20,
                    "verify": False,
                }
                if HAS_CURL_CFFI:
                    kwargs["impersonate"] = "chrome124"

                r = self.session.get(url, **kwargs)

                if r.status_code == 200:
                    self.log(f"Success: {url}", "DEBUG")
                    return r.text

                self.log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code == 429:
                    time.sleep(5)

            except Exception as e:
                self.log(f"Attempt {attempt + 1} failed for {url}: {type(e).__name__} - {e}", "WARNING")
                time.sleep(1.5)

        return None

    # ============================================================
    #  Sitemap
    # ============================================================

    def load_xml(self, url: str) -> Optional[ET.Element]:
        """Fetch and parse an XML sitemap. Strips <script/> before parsing."""
        data = None
        for attempt in range(3):
            try:
                data = self.http_get(url, is_json=False)
                if data:
                    break
            except Exception as e:
                self.log(f"Attempt {attempt + 1} for sitemap failed: {e}", "WARNING")
                time.sleep(2)

        if not data:
            self.log(f"Failed to load XML from {url}", "ERROR")
            return None

        # Strip injected scripts if any
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
                    self.log(f"Found {len(sitemaps)} child sitemaps in {index_url}", "INFO")
                    return sitemaps

        self.log(f"No child sitemaps found in {index_url}", "WARNING")
        return []

    def get_all_sitemaps(self) -> List[str]:
        """Discover all child sitemaps from custom SITEMAP_URL, robots.txt, or default sitemap indexes."""
        if self.sitemap_url:
            self.log(f"Using custom SITEMAP_URL: {self.sitemap_url}")
            children = self.get_child_sitemaps(self.sitemap_url)
            if children:
                return children
            return [self.sitemap_url]

        # 1. Discover sitemap indexes from robots.txt
        index_urls: List[str] = []
        try:
            robots_url = f"{self.curr_url}/robots.txt"
            resp = self.http_get(robots_url)
            if resp:
                for line in resp.splitlines():
                    if line.strip().lower().startswith("sitemap:"):
                        s_url = line.split(":", 1)[1].strip()
                        if s_url and s_url not in index_urls:
                            index_urls.append(s_url)
                if index_urls:
                    self.log(f"Discovered {len(index_urls)} sitemaps from robots.txt: {index_urls}", "INFO")
        except Exception as e:
            self.log(f"Error fetching robots.txt: {e}", "WARNING")

        # 2. Fallback to known Magento sitemaps if robots.txt didn't yield any
        if not index_urls:
            index_urls = [
                f"{self.curr_url}/sitemap.xml",
                f"{self.curr_url}/pub/sitemap/sitemap.xml",
            ]
            self.log(f"Using default sitemap index URLs: {index_urls}", "INFO")

        child_sitemaps: List[str] = []
        for idx_url in index_urls:
            children = self.get_child_sitemaps(idx_url)
            for c in children:
                if c not in child_sitemaps:
                    child_sitemaps.append(c)

        self.log(f"Total discovered child sitemaps: {len(child_sitemaps)}", "INFO")
        return child_sitemaps

    def get_product_urls(self, sitemap_url: str) -> List[str]:
        """Parse a child sitemap and return list of URLs."""
        ns  = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        xml = self.load_xml(sitemap_url)
        if xml is None:
            return []

        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
            if elements:
                urls = [e.text.strip() for e in elements if e.text]
                if urls:
                    return urls

        return []

    # ============================================================
    #  Helpers
    # ============================================================

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
    #  Product Extraction
    # ============================================================

    def extract_unlimitedfurniture_data(self, soup: BeautifulSoup, url: str, html: str) -> List[Dict]:
        """Parse Product JSON-LD block and page metadata from an Unlimited Furniture page."""
        results: List[Dict] = []

        product_obj = None
        category_name = ""
        category_url = ""

        # Find Product JSON-LD & BreadcrumbList
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string
                if not raw:
                    continue
                data = json.loads(raw)

                items_to_check = []
                if isinstance(data, list):
                    items_to_check = data
                elif isinstance(data, dict):
                    if "@graph" in data and isinstance(data["@graph"], list):
                        items_to_check = data["@graph"]
                    else:
                        items_to_check = [data]

                for item in items_to_check:
                    if not isinstance(item, dict):
                        continue
                    item_type = item.get("@type")
                    if item_type in ("Product", "IndividualProduct"):
                        product_obj = item
                    elif item_type == "BreadcrumbList":
                        crumb_list = item.get("itemListElement", [])
                        if isinstance(crumb_list, list) and len(crumb_list) > 1:
                            target_crumb = crumb_list[-1]
                            crumb_item = target_crumb.get("item", {})
                            if isinstance(crumb_item, dict):
                                category_name = crumb_item.get("name", "")
                                category_url = crumb_item.get("@id", "")
            except Exception as e:
                self.log(f"JSON-LD parse warning: {e}", "DEBUG")

        if not product_obj:
            return []

        # Offers & Price
        offers = product_obj.get("offers", {})
        if isinstance(offers, list) and offers:
            offers = offers[0]
        if not isinstance(offers, dict):
            offers = {}

        price = str(
            offers.get("price")
            or offers.get("lowPrice")
            or ""
        ).strip()

        availability = str(offers.get("availability", "")).lower()
        status = "In Stock" if "instock" in availability else "Out of Stock"

        # Brand
        brand_raw = product_obj.get("brand", {})
        brand = brand_raw.get("name", "") if isinstance(brand_raw, dict) else str(brand_raw or "")

        # SKU, MPN, GTIN
        sku = str(product_obj.get("sku", "") or "").strip()
        mpn = str(product_obj.get("mpn", "") or sku).strip()
        gtin = str(
            product_obj.get("gtin13")
            or product_obj.get("gtin")
            or product_obj.get("gtin8")
            or product_obj.get("gtin14")
            or ""
        ).strip()

        # Image
        images = product_obj.get("image", "")
        main_image = images[0] if isinstance(images, list) and images else (images if isinstance(images, str) else "")

        # Additional Properties: Product Type, Collection, Manufacturer, etc.
        add_props = product_obj.get("additionalProperty", [])
        product_type = ""
        collection = ""
        if isinstance(add_props, list):
            for prop in add_props:
                if isinstance(prop, dict):
                    p_name = prop.get("name", "").strip().lower()
                    p_val = prop.get("value", "").strip()
                    if p_name in ("product type", "category") and not product_type:
                        product_type = p_val
                    elif p_name == "collection" and not collection:
                        collection = p_val
                    elif p_name == "manufacturer" and not brand:
                        brand = p_val

        if not category_name and product_type:
            category_name = product_type

        # Product ID from HTML or SKU
        pid_match = (
            re.search(r'name=[\'\"]product[\'\"]\s+value=[\'\"](\d+)[\'\"]', html)
            or re.search(r'\"productId\":\s*\"?(\d+)\"?', html)
            or re.search(r'data-product-id=[\'\"](\d+)[\'\"]', html)
        )
        product_id = pid_match.group(1) if pid_match else sku

        description = str(product_obj.get("description", "") or "").strip()

        results.append({
            "competitor_product_id": product_id,
            "comp_received_name":    product_obj.get("name", "").strip(),
            "comp_received_sku":     sku,
            "brand":                 brand,
            "mpn":                   mpn,
            "category":              category_name,
            "category_url":          category_url,
            "gtin":                  gtin,
            "quantity":              1 if status == "In Stock" else 0,
            "status":                status,
            "competitor_price":      price,
            "group_attr_1":          description[:500],
            "group_attr_2":          collection,
            "main_image":            self.normalize_image(main_image),
            "competitor_url":        url,
            "scraped_date":          self.scraped_date,
        })

        return results

    # ============================================================
    #  CSV Write
    # ============================================================

    def write_row(self, writer: csv.writer, product: Dict):
        """Thread-safe write of one product row to CSV."""
        row = [
            product["competitor_url"],
            product["competitor_product_id"],
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
        failure_path = os.path.join(self.failure_dir, "unlimitedfurniture_failures.csv")
        file_exists  = os.path.isfile(failure_path)
        with self.fail_lock:
            with open(failure_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["URL", "Reason", "Timestamp"])
                w.writerow([url, reason, self.scraped_date])

    def log_skipped_plp(self, url: str):
        """Append a skipped PLP URL to the per-chunk skipped CSV."""
        os.makedirs(self.failure_dir, exist_ok=True)
        skipped_path = os.path.join(self.failure_dir, self.skipped_plp_csv)
        file_exists  = os.path.isfile(skipped_path)
        with self.fail_lock:
            with open(skipped_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["URL", "Reason", "Timestamp", "Sitemap Offset"])
                w.writerow([url, "PLP / non-product URL skipped", self.scraped_date, self.sitemap_offset])

    # ============================================================
    #  Process Single Product
    # ============================================================

    def _is_plp_url(self, url: str) -> bool:
        """Check if URL is clearly a category / PLP / CMS non-product page."""
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        if not path:
            return True

        # Products in Magento end with .html
        if not path.endswith('.html'):
            return True

        # In Magento, categories have multiple path components (/furniture/bedroom.html)
        if '/' in path:
            return True

        # Known root category or CMS .html pages
        root_plps = {
            'kids-furniture.html', 'decor-and-accents.html', 'luxury-mattresses.html',
            'bestsellers.html', 'on-sale.html', 'lifestyle.html', 'tabletop-kitchen.html',
            'shop-by-brand.html', 'instagram.html'
        }
        if path.lower() in root_plps:
            return True

        return False

    def process_product(self, product_url: str, writer: csv.writer):
        """Fetch, extract, and save one unlimitedfurniture product URL."""
        if self._is_plp_url(product_url):
            self.stats["plp_urls_skipped"] += 1
            self.log_skipped_plp(product_url)
            return

        base_url = self.clean_url(product_url)

        if base_url in self.seen:
            return
        self.seen.add(base_url)

        self.log(f"Processing: {base_url}", "DEBUG")

        html = self.http_get(base_url, is_json=False)
        if not html:
            self.stats["errors"] += 1
            self.log_failure(base_url, "HTTP fetch failed")
            return

        soup     = BeautifulSoup(html, "html.parser")
        products = self.extract_unlimitedfurniture_data(soup, base_url, html)

        if not products:
            self.stats["errors"] += 1
            self.log_failure(base_url, "No product data found in JSON-LD")
            return

        for product in products:
            if not product.get("comp_received_name"):
                continue
            try:
                self.write_row(writer, product)
                self.stats["products_fetched"] += 1
                self.log(
                    f"Saved [{product['competitor_product_id']}] "
                    f"{product['comp_received_name'][:60]} | ${product['competitor_price']}"
                )
            except Exception as e:
                self.log(f"Row write error: {e}", "ERROR")
                self.stats["errors"] += 1

        time.sleep(self.request_delay)
        self.stats["urls_processed"] += 1

    # ============================================================
    #  Run — main orchestrator
    # ============================================================

    def run(self):
        self.log("=" * 60)
        self.log("unlimitedfurniture Parallel Bulk Scraper")
        self.log(f"Timestamp:            {self.scraped_date}")
        self.log(f"curl_cffi Available:  {HAS_CURL_CFFI}")
        self.log(f"Base URL:             {self.curr_url}")
        self.log(f"Sitemap Offset:       {self.sitemap_offset}")
        self.log(f"Max Sitemaps:         {self.max_sitemaps or 'All'}")
        self.log(f"Max URLs per Sitemap: {self.max_urls_per_sitemap or 'All'}")
        self.log(f"Max Workers:          {self.max_workers}")
        self.log(f"Request Delay:        {self.request_delay}s")
        self.log("=" * 60)

        all_sitemaps = self.get_all_sitemaps()

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

            for sitemap_url in sitemaps_to_run:
                self.stats["sitemaps_processed"] += 1
                self.log(f"Sitemap {self.stats['sitemaps_processed']}/{len(sitemaps_to_run)}: {sitemap_url}")

                urls = self.get_product_urls(sitemap_url)
                if not urls:
                    continue
                self.all_product_urls.extend(urls)

                # Separate candidate products from PLP / non-product URLs
                candidate_products = []
                for u in urls:
                    if self._is_plp_url(u):
                        self.stats["plp_urls_skipped"] += 1
                        self.log_skipped_plp(u)
                    else:
                        candidate_products.append(u)

                if self.max_urls_per_sitemap > 0:
                    self.log(f"Limiting to {self.max_urls_per_sitemap} of {len(candidate_products)} candidate product URLs")
                    urls_to_scrape = candidate_products[:self.max_urls_per_sitemap]
                else:
                    self.log(f"Found {len(candidate_products)} candidate product URLs in sitemap")
                    urls_to_scrape = candidate_products

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self.process_product, url, writer)
                        for url in urls_to_scrape
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            self.log(f"Thread error: {e}", "ERROR")
                            self.stats["errors"] += 1

                gc.collect()

        self.log("=" * 60)
        self.log("SCRAPING COMPLETE")
        self.log(f"  Sitemaps processed:  {self.stats['sitemaps_processed']}")
        self.log(f"  URLs processed:      {self.stats['urls_processed']}")
        self.log(f"  Products saved:      {self.stats['products_fetched']}")
        self.log(f"  Errors:              {self.stats['errors']}")
        self.log(f"  PLP URLs skipped:    {self.stats['plp_urls_skipped']}")
        if self.stats["urls_processed"] > 0:
            rate = self.stats["products_fetched"] / self.stats["urls_processed"] * 100
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

    _uf_scraper = unlimitedFurnitureScraper()
    _uf_scraper.run()

    # Save per-chunk URL list for artifact upload & FTP tracking
    if hasattr(_uf_scraper, 'all_product_urls') and _uf_scraper.all_product_urls:
        try:
            _url_f = f"url_list_chunk_{_uf_scraper.sitemap_offset}.txt"
            with open(_url_f, "w", encoding="utf-8") as _uf:
                _uf.write("\n".join(_uf_scraper.all_product_urls))
            _uf_scraper.log(f"[URL-LIST] Saved {len(_uf_scraper.all_product_urls)} URLs to {_url_f}")
        except Exception as _exc:
            sys.stderr.write(f"[URL-LIST] Warning: {_exc}\n")
