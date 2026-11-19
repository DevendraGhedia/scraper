#!/usr/bin/env python3
"""
Ashley Furniture (Direct) Product Scraper
Scrapes products directly from https://www.ashleyfurniture.com via curl_cffi with Chrome TLS impersonation.
Supports Sitemap extraction, URL files, direct URLs, multi-threading, and GitHub Actions parallel matrix jobs.
"""

import os
import sys
import csv
import time
import json
import logging
import argparse
import threading
from typing import List, Dict, Optional, Any, Set
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from curl_cffi import requests

# Configure logging
logger = logging.getLogger("ashley_direct")
logger.setLevel(logging.INFO)
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


class AshleyDirectScraper:
    def __init__(
        self,
        output_file: str = "ashley_direct_products.csv",
        max_workers: int = 8,
        request_delay: float = 0.2,
        impersonate: str = "chrome120",
        timeout: int = 20,
        max_retries: int = 3,
    ):
        self.output_file = output_file
        self.max_workers = max_workers
        self.request_delay = request_delay
        self.impersonate = impersonate
        self.timeout = timeout
        self.max_retries = max_retries

        self.csv_lock = threading.Lock()
        self.session = requests.Session()
        self.scraped_count = 0
        self.failed_count = 0
        self.seen_urls: Set[str] = set()

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
            # Fallback to known sub-sitemaps (0 to 6)
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
                    if elem.text and "/p/" in elem.text
                ]
                logger.info(f"Found {len(urls)} product URLs in {sitemap_url}")
                if max_urls_per_sitemap > 0:
                    urls = urls[:max_urls_per_sitemap]
                    logger.info(f"Capped to {len(urls)} URLs for this sitemap")

                for u in urls:
                    if u not in self.seen_urls:
                        self.seen_urls.add(u)
                        product_urls.append(u)

                if max_total_urls > 0 and len(product_urls) >= max_total_urls:
                    product_urls = product_urls[:max_total_urls]
                    break
            except Exception as e:
                logger.error(f"Error parsing sitemap {sitemap_url}: {e}")

        logger.info(f"Extracted total {len(product_urls)} unique product URLs from sitemaps")
        return product_urls

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

        # 2. Basic identifiers
        sku = prod_data.get("sku", "").strip()
        product_name = prod_data.get("name", "").strip()
        product_id = prod_data.get("@id", "").split("/")[-1] or sku

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

        # 8. Product Type (Set vs Simple)
        name_lower = product_name.lower()
        cat_lower = category_name.lower()
        if "set" in name_lower or "bundle" in name_lower or "set" in cat_lower or "piece" in name_lower:
            product_type = "bundle"
        else:
            product_type = "simple"

        # 9. Collection Name
        # In Ashley products, collection name is typically the leading word/name
        # e.g. "Bronyan King Panel Bed" -> "Bronyan"
        words = product_name.split()
        collection_name = words[0] if len(words) > 1 and len(words[0]) >= 3 else ""

        # 10. Descriptions & Attributes
        description = prod_data.get("description", "").strip()
        color = prod_data.get("color", "")
        scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        return {
            "Ref Product URL": url,
            "Ref Product ID": product_id,
            "Ref Variant ID": "",
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
            "Ref Group Attr 2": "",
            "Ref Images": all_imgs,
            "Ref Dimensions": dim_str,
            "Ref Status": status,
            "Ref Highlights": description,
            "Date Scrapped": scraped_date,
        }

    def scrape_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Worker function to scrape a single product URL."""
        if self.request_delay > 0:
            time.sleep(self.request_delay)

        resp = self.fetch_with_retry(url)
        if not resp:
            self.failed_count += 1
            return None

        item = self.parse_product_page(url, resp.text)
        if not item:
            self.failed_count += 1
            return None

        # Thread-safe write to CSV
        with self.csv_lock:
            with open(self.output_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writerow(item)
            self.scraped_count += 1

        return item

    def run(self, urls: List[str]):
        """Run multi-threaded scraping across a list of URLs."""
        total = len(urls)
        logger.info(f"Starting product scraping for {total} URLs with {self.max_workers} workers...")
        logger.info(f"Output CSV: {self.output_file}")

        # Initialize CSV header if file does not exist
        if not os.path.exists(self.output_file) or os.path.getsize(self.output_file) == 0:
            os.makedirs(os.path.dirname(os.path.abspath(self.output_file)), exist_ok=True)
            with open(self.output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writeheader()

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.scrape_url, url): url for url in urls}

            completed = 0
            for future in as_completed(future_to_url):
                completed += 1
                if completed % 25 == 0 or completed == total:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    pct = (completed / total) * 100
                    logger.info(
                        f"Progress: {completed}/{total} ({pct:.1f}%) | "
                        f"Scraped: {self.scraped_count} | Failed: {self.failed_count} | "
                        f"Rate: {rate:.1f} urls/sec"
                    )

        total_elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"Scraping Completed in {total_elapsed:.2f}s ({total_elapsed/60:.2f} mins)")
        logger.info(f"Total Processed: {total}")
        logger.info(f"Successfully Scraped: {self.scraped_count}")
        logger.info(f"Failed: {self.failed_count}")
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

    # Limits & Tuning
    parser.add_argument("--max-products", type=int, default=0, help="Max total products to scrape (0 = unlimited)")
    parser.add_argument(
        "--max-urls-per-sitemap", type=int, default=0, help="Max URLs per sitemap to collect (0 = unlimited)"
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
    )

    urls_to_scrape = []

    # 1. Direct URLs via argument
    if args.urls:
        urls_to_scrape = [u.strip() for u in args.urls.split(",") if u.strip().startswith("http")]
        logger.info(f"Loaded {len(urls_to_scrape)} URLs from command-line argument")

    # 2. URLs from file
    elif args.urls_file:
        if not os.path.exists(args.urls_file):
            logger.error(f"URLs file not found: {args.urls_file}")
            sys.exit(1)
        with open(args.urls_file, "r", encoding="utf-8") as f:
            if args.urls_file.endswith(".json"):
                data = json.load(f)
                urls_to_scrape = data if isinstance(data, list) else data.get("urls", [])
            else:
                urls_to_scrape = [line.strip() for line in f if line.strip().startswith("http")]
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
            # Default to sitemap 0 for safe execution unless requested otherwise
            default_idx = int(os.getenv("SITEMAP_INDEX", "0"))
            sitemap_urls = scraper.get_product_sitemap_urls(specific_indices=[default_idx])

        urls_to_scrape = scraper.extract_urls_from_sitemaps(
            sitemap_urls,
            max_urls_per_sitemap=args.max_urls_per_sitemap,
            max_total_urls=args.max_products,
        )

    if args.max_products > 0 and len(urls_to_scrape) > args.max_products:
        urls_to_scrape = urls_to_scrape[: args.max_products]
        logger.info(f"Limited URLs list to {len(urls_to_scrape)} items (--max-products)")

    if not urls_to_scrape:
        logger.error("No valid URLs found to scrape!")
        sys.exit(1)

    scraper.run(urls_to_scrape)


if __name__ == "__main__":
    main()
