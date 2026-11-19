#!/usr/bin/env python3
"""
Ashley Furniture (Direct) Product Scraper
Scrapes products directly from https://www.ashleyfurniture.com via curl_cffi with Chrome TLS impersonation.
Supports Sitemap extraction, URL files, direct URLs, multi-threading, variant & bundle/set discovery, and GitHub Actions parallel matrix jobs.
"""

import os
import sys
import csv
import time
import json
import queue
import logging
import argparse
import threading
import urllib.parse
from typing import List, Dict, Optional, Any, Set
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
import re

from bs4 import BeautifulSoup
from curl_cffi import requests

# Configure logging
logger = logging.getLogger("ashley_direct")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

CSV_HEADERS = [
    "Ref Product URL",
    "Ref Product ID",
    "Ref Variant ID",
    "Ref Category",
    "Ref Category URL",
    "Product Type",
    "Ref Brand Name",
    "Collection Name",
    "Ref Product Name",
    "Ref SKU",
    "Ref MPN",
    "Ref GTIN",
    "Ref Price",
    "Ref Main Image",
    "Ref Quantity",
    "Ref Group Attr 1",
    "Ref Group Attr 2",
    "Ref Images",
    "Ref Dimensions",
    "Ref Status",
    "Ref Highlights",
    "Date Scrapped",
]

SITEMAP_INDEX_URL = "https://www.ashleyfurniture.com/sitemap_index.xml"
BASE_URL = "https://www.ashleyfurniture.com"

SIZE_PATTERNS = [
    r"\bCalifornia King\b",
    r"\bCal King\b",
    r"\bTwin XL\b",
    r"\bTwin / Twin\b",
    r"\bFull / Full\b",
    r"\bTwin over Full\b",
    r"\bKing\b",
    r"\bQueen\b",
    r"\bFull\b",
    r"\bTwin\b",
]


def normalize_url(url: str) -> str:
    """Normalize Ashley product URL by removing query strings and anchors."""
    u = url.split("?")[0].split("#")[0].strip()
    if not u.startswith("http"):
        u = urllib.parse.urljoin(BASE_URL, u)
    return u


def is_valid_product_url(url: str) -> bool:
    """Validate that the URL is an Ashley product page."""
    return "/p/" in url and url.endswith(".html")


class AshleyDirectScraper:
    def __init__(
        self,
        output_file: str = "ashley_direct_products.csv",
        max_workers: int = 8,
        request_delay: float = 0.2,
        impersonate: str = "chrome120",
        timeout: int = 25,
        max_retries: int = 3,
        include_variants: bool = True,
        max_products: int = 0,
    ):
        self.output_file = output_file
        self.max_workers = max_workers
        self.request_delay = request_delay
        self.impersonate = impersonate
        self.timeout = timeout
        self.max_retries = max_retries
        self.include_variants = include_variants
        self.max_products = max_products

        self.csv_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.active_lock = threading.Lock()

        self.session = requests.Session()
        self.scraped_count = 0
        self.failed_count = 0
        self.seen_urls: Set[str] = set()
        self.scraped_skus: Set[str] = set()
        self.url_queue: queue.Queue = queue.Queue()
        self.active_workers = 0
        self.stop_event = threading.Event()

    def fetch_with_retry(self, url: str) -> Optional[requests.Response]:
        """Fetch URL using curl_cffi with browser TLS impersonation and exponential retries."""
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(
                    url,
                    impersonate=self.impersonate,
                    timeout=self.timeout,
                    headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Referer": "https://www.ashleyfurniture.com/",
                    },
                )
                if resp.status_code == 200:
                    return resp
                elif resp.status_code in (429, 500, 502, 503, 504):
                    wait = attempt * 2
                    logger.warning(f"HTTP {resp.status_code} for {url}. Backing off {wait}s (attempt {attempt}/{self.max_retries})...")
                    time.sleep(wait)
                else:
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                    return None
            except Exception as e:
                wait = attempt * 2
                logger.warning(f"Error fetching {url}: {e}. Retrying in {wait}s (attempt {attempt}/{self.max_retries})...")
                time.sleep(wait)

        return None

    def get_product_sitemap_urls(self, specific_indices: Optional[List[int]] = None) -> List[str]:
        """Fetch sitemap index and return product sub-sitemap URLs."""
        logger.info(f"Fetching sitemap index: {SITEMAP_INDEX_URL}")
        resp = self.fetch_with_retry(SITEMAP_INDEX_URL)
        if not resp:
            logger.error(f"Failed to fetch sitemap index from {SITEMAP_INDEX_URL}")
            all_sitemaps = [f"https://www.ashleyfurniture.com/sitemap_{i}-product.xml" for i in range(7)]
        else:
            try:
                root = ET.fromstring(resp.content)
                all_sitemaps = []
                for child in root:
                    loc = child.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
                    if loc is not None and loc.text and "-product" in loc.text:
                        all_sitemaps.append(loc.text.strip())
            except Exception as e:
                logger.error(f"Failed to parse sitemap index XML: {e}")
                all_sitemaps = [f"https://www.ashleyfurniture.com/sitemap_{i}-product.xml" for i in range(7)]

        if specific_indices is not None and len(specific_indices) > 0:
            filtered = []
            for idx in specific_indices:
                target = f"sitemap_{idx}-product.xml"
                match = [s for s in all_sitemaps if target in s]
                if match:
                    filtered.append(match[0])
                else:
                    filtered.append(f"https://www.ashleyfurniture.com/sitemap_{idx}-product.xml")
            all_sitemaps = filtered

        logger.info(f"Targeting {len(all_sitemaps)} product sitemap(s): {all_sitemaps}")
        return all_sitemaps

    def extract_urls_from_sitemaps(
        self, sitemap_urls: List[str], max_urls_per_sitemap: int = 0, max_total_urls: int = 0
    ) -> List[str]:
        """Extract product URLs from given XML product sitemaps."""
        product_urls = []
        seen = set()
        for sitemap_url in sitemap_urls:
            logger.info(f"Fetching sitemap: {sitemap_url}")
            resp = self.fetch_with_retry(sitemap_url)
            if not resp:
                logger.error(f"Failed to load sitemap: {sitemap_url}")
                continue

            try:
                root = ET.fromstring(resp.content)
                urls = [
                    elem.text.strip()
                    for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
                    if elem.text and is_valid_product_url(elem.text.strip())
                ]
                logger.info(f"Found {len(urls)} product URLs in {sitemap_url}")
                if max_urls_per_sitemap > 0:
                    urls = urls[:max_urls_per_sitemap]
                    logger.info(f"Capped to {len(urls)} URLs for this sitemap")

                for u in urls:
                    norm = normalize_url(u)
                    if norm not in seen:
                        seen.add(norm)
                        product_urls.append(norm)

                if max_total_urls > 0 and len(product_urls) >= max_total_urls:
                    product_urls = product_urls[:max_total_urls]
                    break
            except Exception as e:
                logger.error(f"Error parsing sitemap {sitemap_url}: {e}")

        logger.info(f"Extracted total {len(product_urls)} unique product URLs from sitemaps")
        return product_urls

    @staticmethod
    def extract_related_urls(url: str, html: str) -> List[str]:
        """Extract all variant URLs, swatch links, and set/bundle pieces from page DOM & scripts."""
        found = set()
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return []

        # 1. Swatches, chips, and links in DOM
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "/p/" in href:
                norm = normalize_url(href)
                if is_valid_product_url(norm):
                    found.add(norm)

        # 2. SvelteKit scripts and embedded JSON
        for s in soup.find_all("script"):
            t = s.string or ""
            if not t or "/p/" not in t:
                continue

            # Full URLs
            matches = re.findall(
                r"[\"\x27](https://www\.ashleyfurniture\.com/p/[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+\.html)[\"\x27]", t
            )
            for m in matches:
                norm = normalize_url(m)
                if is_valid_product_url(norm):
                    found.add(norm)

            # Relative URLs
            rel_matches = re.findall(r"[\"\x27](/p/[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+\.html)[\"\x27]", t)
            for rm in rel_matches:
                norm = normalize_url(rm)
                if is_valid_product_url(norm):
                    found.add(norm)

        # Do not include the current URL itself in discovered variants
        current_norm = normalize_url(url)
        found.discard(current_norm)
        return sorted(found)

    @staticmethod
    def parse_product_page(url: str, html: str) -> Optional[Dict[str, Any]]:
        """Parse product page HTML into the standardized 22-column dictionary."""
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return None

        prod_data = None
        breadcrumbs = []

        # 1. Parse JSON-LD scripts
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    if data.get("@type") == "Product":
                        prod_data = data
                    elif data.get("@type") == "BreadcrumbList":
                        for item in data.get("itemListElement", []):
                            name = item.get("name")
                            if name and name.lower() != "home":
                                breadcrumbs.append(name.strip())
            except Exception:
                continue

        if not prod_data:
            return None

        # 2. Identifiers & Master ID
        sku = prod_data.get("sku", "").strip()
        product_name = prod_data.get("name", "").strip()

        # Find masterId in scripts if this is a variant/child
        master_id = ""
        script_texts = []
        for s in soup.find_all("script"):
            t = s.string or ""
            if not t:
                continue
            script_texts.append(t)
            if not master_id:
                m = re.findall(r"[\"\x27]masterId[\"\x27]\s*:\s*[\"\x27]([^\"]+)[\"\x27]", t)
                if m:
                    master_id = m[0]
                else:
                    m2 = re.findall(
                        r"[\"\x27]?[a-zA-Z0-9_]*master[a-zA-Z0-9_]*[\"\x27]?\s*:\s*[\"\x27]([A-Za-z0-9_\-]+-Master)[\"\x27]",
                        t,
                        re.IGNORECASE,
                    )
                    if m2:
                        master_id = m2[0]

        if master_id:
            product_id = master_id
            variant_id = sku
        else:
            product_id = sku
            variant_id = sku

        # 3. Brand
        brand_obj = prod_data.get("brand", {})
        brand_name = brand_obj.get("name", "Ashley") if isinstance(brand_obj, dict) else str(brand_obj or "Ashley")

        # 4. Price & Availability
        offers = prod_data.get("offers", {})
        if isinstance(offers, dict):
            price = offers.get("price", "")
            avail = offers.get("availability", "")
        else:
            price = ""
            avail = ""
        status = "In Stock" if "InStock" in avail else ("Out of Stock" if "OutOfStock" in avail else avail)

        # 5. Category
        category_name = " > ".join(breadcrumbs) if breadcrumbs else prod_data.get("category", "")
        category_url = ""

        # 6. Dimensions & Weight
        w = prod_data.get("width", {})
        d = prod_data.get("depth", {})
        h = prod_data.get("height", {})
        wt = prod_data.get("weight", {})

        dim_parts = []
        if isinstance(w, dict) and "value" in w:
            dim_parts.append(f'{w["value"]}"W')
        if isinstance(d, dict) and "value" in d:
            dim_parts.append(f'{d["value"]}"D')
        if isinstance(h, dict) and "value" in h:
            dim_parts.append(f'{h["value"]}"H')

        dim_str = " x ".join(dim_parts)
        if isinstance(wt, dict) and "value" in wt:
            dim_str = f"{dim_str} - {wt['value']}lbs" if dim_str else f"{wt['value']}lbs"

        # 7. Images
        images = prod_data.get("image", [])
        if isinstance(images, str):
            images = [images]
        main_img = images[0] if images else ""
        all_imgs = " | ".join(images)

        # 8. Product Type (Bundle vs Variant vs Simple)
        name_lower = product_name.lower()
        cat_lower = category_name.lower()
        if any(k in name_lower or k in cat_lower for k in ["set", "bundle", "piece with", "dresser and nightstand"]):
            product_type = "bundle"
        elif master_id and master_id != sku:
            product_type = "variant"
        else:
            product_type = "simple"

        # 9. Collection Name
        words = product_name.split()
        collection_name = words[0] if len(words) > 1 and len(words[0]) >= 3 else ""

        # 10. Descriptions & Attributes (Color & Size)
        description = prod_data.get("description", "").strip()
        color = prod_data.get("color", "")

        # Extract Size attribute
        size = ""
        all_script_text = " ".join(script_texts)
        if sku:
            pattern = rf"productId\s*:\s*[\"\x27]{re.escape(sku)}[\"\x27][^{{}}]*variationValues\s*:\s*\{{([^}}]+)\}}"
            m = re.search(pattern, all_script_text)
            if m:
                val_text = m.group(1)
                sm = re.search(r"size\d*\s*:\s*[\"\x27]([^\"\x27]+)[\"\x27]", val_text, re.IGNORECASE)
                if sm:
                    size = sm.group(1).strip()

        if not size:
            for pat in SIZE_PATTERNS:
                m = re.search(pat, product_name, re.IGNORECASE)
                if m:
                    size = m.group(0)
                    break

        scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        return {
            "Ref Product URL": url,
            "Ref Product ID": product_id,
            "Ref Variant ID": variant_id,
            "Ref Category": category_name,
            "Ref Category URL": category_url,
            "Product Type": product_type,
            "Ref Brand Name": brand_name,
            "Collection Name": collection_name,
            "Ref Product Name": product_name,
            "Ref SKU": sku,
            "Ref MPN": sku,
            "Ref GTIN": "",
            "Ref Price": price,
            "Ref Main Image": main_img,
            "Ref Quantity": "",
            "Ref Group Attr 1": color,
            "Ref Group Attr 2": size,
            "Ref Images": all_imgs,
            "Ref Dimensions": dim_str,
            "Ref Status": status,
            "Ref Highlights": description,
            "Date Scrapped": scraped_date,
        }

    def process_url(self, url: str):
        """Fetch, parse, save, and discover new variants/pieces from a URL."""
        if self.stop_event.is_set():
            return

        if self.request_delay > 0:
            time.sleep(self.request_delay)

        resp = self.fetch_with_retry(url)
        if not resp:
            with self.state_lock:
                self.failed_count += 1
            return

        item = self.parse_product_page(url, resp.text)
        if not item:
            with self.state_lock:
                self.failed_count += 1
            return

        sku = item.get("Ref SKU", "").strip()

        # Save to CSV if SKU is not already written
        should_save = False
        with self.state_lock:
            if sku and sku not in self.scraped_skus:
                self.scraped_skus.add(sku)
                should_save = True
            elif not sku and url not in self.scraped_skus:
                self.scraped_skus.add(url)
                should_save = True

        if should_save:
            with self.csv_lock:
                with open(self.output_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                    writer.writerow(item)
                with self.state_lock:
                    self.scraped_count += 1
                    if self.max_products > 0 and self.scraped_count >= self.max_products:
                        self.stop_event.set()

        # Discover & enqueue child variants and set pieces if enabled
        if self.include_variants and not self.stop_event.is_set():
            discovered = self.extract_related_urls(url, resp.text)
            for new_url in discovered:
                with self.state_lock:
                    if new_url not in self.seen_urls:
                        self.seen_urls.add(new_url)
                        if not self.stop_event.is_set():
                            self.url_queue.put(new_url)

    def _worker(self):
        """Worker thread loop consuming from url_queue."""
        while not self.stop_event.is_set():
            try:
                url = self.url_queue.get(timeout=2.0)
            except queue.Empty:
                with self.active_lock:
                    if self.active_workers == 0 and self.url_queue.empty():
                        break
                continue

            with self.active_lock:
                self.active_workers += 1

            try:
                self.process_url(url)
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
            finally:
                self.url_queue.task_done()
                with self.active_lock:
                    self.active_workers -= 1

    def run(self, initial_urls: List[str]):
        """Run multi-threaded crawling and variant discovery across queue."""
        logger.info(
            f"Starting scraper with {len(initial_urls)} seed URLs | "
            f"Workers: {self.max_workers} | Variants: {self.include_variants}"
        )
        logger.info(f"Output CSV: {self.output_file}")

        # Initialize CSV header if file does not exist
        if not os.path.exists(self.output_file) or os.path.getsize(self.output_file) == 0:
            os.makedirs(os.path.dirname(os.path.abspath(self.output_file)), exist_ok=True)
            with open(self.output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writeheader()

        # Seed queue
        for u in initial_urls:
            norm = normalize_url(u)
            if norm not in self.seen_urls:
                self.seen_urls.add(norm)
                self.url_queue.put(norm)

        start_time = time.time()

        # Start worker threads
        threads = []
        for _ in range(self.max_workers):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            threads.append(t)

        # Monitor progress until all workers finish
        last_log = time.time()
        while any(t.is_alive() for t in threads):
            time.sleep(1.0)
            now = time.time()
            if now - last_log >= 5.0 or self.stop_event.is_set():
                last_log = now
                elapsed = now - start_time
                qsize = self.url_queue.qsize()
                with self.state_lock:
                    sc = self.scraped_count
                    fc = self.failed_count
                    seen = len(self.seen_urls)
                rate = sc / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Progress: Scraped={sc} | Failed={fc} | Queue={qsize} | Discovered={seen} | Rate={rate:.1f} items/sec"
                )
                if self.stop_event.is_set() and qsize == 0:
                    break

        # Wait for threads to exit
        for t in threads:
            t.join(timeout=3.0)

        total_elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"Scraping Completed in {total_elapsed:.2f}s ({total_elapsed/60:.2f} mins)")
        logger.info(f"Total Unique Scraped: {self.scraped_count}")
        logger.info(f"Total Discovered URLs: {len(self.seen_urls)}")
        logger.info(f"Failed Requests: {self.failed_count}")
        logger.info(f"Saved to: {self.output_file}")
        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Ashley Furniture Direct Scraper (ashleyfurniture.com)")

    # Execution Modes
    parser.add_argument("--sitemaps", type=str, default="", help="Comma-separated sitemap indices, e.g. '0,1' or '0'")
    parser.add_argument("--all-sitemaps", action="store_true", help="Scrape all 7 product sitemaps")
    parser.add_argument("--sitemap-url", type=str, default="", help="Specific XML sitemap URL")
    parser.add_argument("--urls-file", type=str, default="", help="Path to text or JSON file containing product URLs")
    parser.add_argument("--urls", type=str, default="", help="Comma-separated direct product URLs")

    # Variants & Bundle Options
    parser.add_argument(
        "--include-variants",
        action="store_true",
        default=True,
        help="Extract and scrape all child variants (sizes, colors) and set/bundle pieces (default: True)",
    )
    parser.add_argument(
        "--no-variants",
        dest="include_variants",
        action="store_false",
        help="Disable automatic crawling of child variants and set pieces",
    )

    # Limits & Tuning
    parser.add_argument(
        "--max-products", type=int, default=0, help="Max total products/variants to scrape (0 = unlimited)"
    )
    parser.add_argument(
        "--max-urls-per-sitemap", type=int, default=0, help="Max seed URLs per sitemap to collect (0 = unlimited)"
    )
    parser.add_argument(
        "--workers", type=int, default=int(os.getenv("MAX_WORKERS", "8")), help="Concurrent worker threads"
    )
    parser.add_argument(
        "--delay", type=float, default=float(os.getenv("REQUEST_DELAY", "0.2")), help="Delay between requests in seconds"
    )
    parser.add_argument("--output", type=str, default="output/ashley_direct_products.csv", help="Output CSV path")

    args = parser.parse_args()

    scraper = AshleyDirectScraper(
        output_file=args.output,
        max_workers=args.workers,
        request_delay=args.delay,
        include_variants=args.include_variants,
        max_products=args.max_products,
    )

    urls_to_scrape = []

    # 1. Direct URLs via argument
    if args.urls:
        urls_to_scrape = [u.strip() for u in args.urls.split(",") if is_valid_product_url(u.strip())]
        logger.info(f"Loaded {len(urls_to_scrape)} URLs from command-line argument")

    # 2. URLs from file
    elif args.urls_file:
        if not os.path.exists(args.urls_file):
            logger.error(f"URLs file not found: {args.urls_file}")
            sys.exit(1)
        with open(args.urls_file, "r", encoding="utf-8") as f:
            if args.urls_file.endswith(".json"):
                data = json.load(f)
                raw_list = data if isinstance(data, list) else data.get("urls", [])
            else:
                raw_list = [line.strip() for line in f if line.strip().startswith("http")]
        urls_to_scrape = [u for u in raw_list if is_valid_product_url(u)]
        logger.info(f"Loaded {len(urls_to_scrape)} URLs from {args.urls_file}")

    # 3. Sitemaps mode
    else:
        if args.sitemap_url:
            sitemap_urls = [args.sitemap_url]
        elif args.all_sitemaps:
            sitemap_urls = scraper.get_product_sitemap_urls()
        elif args.sitemaps:
            indices = [int(i.strip()) for i in args.sitemaps.split(",") if i.strip().isdigit()]
            sitemap_urls = scraper.get_product_sitemap_urls(specific_indices=indices)
        else:
            default_idx = int(os.getenv("SITEMAP_INDEX", "0"))
            sitemap_urls = scraper.get_product_sitemap_urls(specific_indices=[default_idx])

        urls_to_scrape = scraper.extract_urls_from_sitemaps(
            sitemap_urls,
            max_urls_per_sitemap=args.max_urls_per_sitemap,
            max_total_urls=args.max_products,
        )

    if not urls_to_scrape:
        logger.error("No valid URLs found to scrape!")
        sys.exit(1)

    scraper.run(urls_to_scrape)


if __name__ == "__main__":
    main()
