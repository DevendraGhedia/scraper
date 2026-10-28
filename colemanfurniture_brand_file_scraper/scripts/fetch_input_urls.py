#!/usr/bin/env python3
import os
import sys
import argparse
import logging
import csv
import json
import tempfile
import ftplib
from pathlib import Path
from typing import List

# Set up logger
logger = logging.getLogger("fetch_input_urls")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

sys.path.insert(0, str(Path(__file__).parent.parent))

def save_urls_to_csv(urls: List[str], output_file: str):
    """Writes a list of URLs to the standard remaining_merged.csv structure."""
    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    
    # Filter and clean URLs
    clean_urls = []
    seen = set()
    for u in urls:
        u_str = str(u).strip()
        if u_str and u_str.startswith("http") and u_str not in seen:
            seen.add(u_str)
            clean_urls.append(u_str)

    header = ["url", "status", "error_type", "error_message", "failed_at", "job_id", "chunk_id"]
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for u in clean_urls:
            writer.writerow([u, "", "", "", "", "", ""])
            
    logger.info(f"Successfully saved {len(clean_urls)} unique URLs to {output_file}")
    return len(clean_urls)

def fetch_from_ftp(host: str, port: int, user: str, pass_: str, remote_dir: str, target_filename: str = "") -> List[str]:
    logger.info(f"Connecting to FTP {host}:{port} as user '{user}'...")
    ftp = ftplib.FTP()
    ftp.connect(host, port, timeout=30)
    ftp.login(user, pass_)
    
    # Support relative sub-paths in target_filename (e.g., "input_urls/coleman FTP urls.csv")
    if target_filename:
        target_filename = target_filename.strip()
        if "/" in target_filename or "\\" in target_filename:
            parts = target_filename.replace("\\", "/").lstrip("/").split("/")
            sub_dir = "/".join(parts[:-1])
            target_filename = parts[-1]
            if remote_dir:
                remote_dir = remote_dir.rstrip("/") + "/" + sub_dir
            else:
                remote_dir = "/" + sub_dir

    if remote_dir:
        ftp.cwd(remote_dir)
        logger.info(f"Changed directory to remote path: {remote_dir}")
        
    items = ftp.nlst()
    logger.info(f"Found {len(items)} items in remote FTP directory.")
    
    target_file = ""
    if target_filename and target_filename in items:
        target_file = target_filename
    elif target_filename:
        # Check case-insensitive match
        for it in items:
            if it.lower() == target_filename.lower():
                target_file = it
                break
        if not target_file:
            raise FileNotFoundError(f"Specified target file '{target_filename}' not found in FTP folder '{remote_dir}'. Available: {items[:20]}...")
    else:
        # Search for .csv files
        csv_files = [it for it in items if it.lower().endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(f"No .csv files found in FTP directory '{remote_dir}'. Available items: {items[:20]}...")
        # Default to the first CSV file found
        target_file = csv_files[0]
        logger.info(f"No filename specified. Auto-selected CSV file from FTP: {target_file}")
        
    logger.info(f"Downloading '{target_file}' from FTP...")
    urls = []
    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(tmp_fd, "wb") as tmp:
            ftp.retrbinary(f"RETR {target_file}", tmp.write)
        ftp.quit()
        logger.info("FTP download complete. Parsing CSV file...")
        
        with open(tmp_name, "r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.reader(f)
            header = None
            url_idx = -1
            
            # Priority column names
            priority_cols = ["item_url", "ref product url", "product url", "product_url", "url", "link", "product_link", "target_url", "item url"]
            
            for row in reader:
                if not row:
                    continue
                if header is None:
                    header = [c.strip().lower() for c in row]
                    # Find matching column by priority
                    for pcol in priority_cols:
                        for idx, col in enumerate(header):
                            if col == pcol:
                                url_idx = idx
                                break
                        if url_idx != -1:
                            break
                            
                    # Fallback: check if any column contains 'url' or 'link'
                    if url_idx == -1:
                        for idx, col in enumerate(header):
                            if "url" in col or "link" in col:
                                url_idx = idx
                                break
                                
                    if url_idx != -1:
                        logger.info(f"Selected column index {url_idx} ('{header[url_idx]}') for product URLs")
                    else:
                        # Check if first row is data without header
                        if row[0].strip().startswith("http"):
                            urls.append(row[0].strip())
                            url_idx = 0
                    continue
                
                if url_idx != -1 and len(row) > url_idx:
                    u = row[url_idx].strip()
                    if u.startswith("http"):
                        urls.append(u)
                else:
                    # Check fields for http
                    for cell in row:
                        c = cell.strip()
                        if c.startswith("http"):
                            urls.append(c)
                            break
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
            
    logger.info(f"Extracted {len(urls)} URLs from FTP file '{target_file}'")
    return urls

def fetch_from_sitemap(sitemap_url: str) -> List[str]:
    from utils.sitemap_processor import SitemapProcessor
    logger.info(f"Fetching sitemap from: {sitemap_url}")
    processor = SitemapProcessor()
    
    all_sitemaps = processor.extract_all_sitemaps(sitemap_url)
    logger.info(f"Found {len(all_sitemaps)} sitemaps/URLs from main sitemap")
    
    product_urls = []
    for sm in all_sitemaps:
        if sm.startswith("http"):
            if any(sm.endswith(ext) for ext in [".xml", ".xml.gz", ".gz"]) or "sitemap" in sm.lower():
                try:
                    sub_urls = processor.extract_all_sitemaps(sm)
                    for u in sub_urls:
                        if u.startswith("http") and not any(u.endswith(ext) for ext in [".xml", ".xml.gz", ".gz"]):
                            product_urls.append(u)
                except Exception as e:
                    logger.warning(f"Error extracting sub-sitemap {sm}: {e}")
            else:
                product_urls.append(sm)
                
    logger.info(f"Extracted total {len(product_urls)} product URLs from sitemap")
    return product_urls

def fetch_from_api(base_api: str, start_page: int, end_page: int, output_dir: str) -> List[str]:
    logger.info(f"Collecting URLs via Ashley API: {base_api} (pages {start_page} to {end_page})...")
    os.makedirs(output_dir, exist_ok=True)
    
    cmd = [
        sys.executable,
        str(Path(__file__).parent / "run_ashley_scraper.py"),
        "--base-api", base_api,
        "--start-page", str(start_page),
        "--end-page", str(end_page),
        "--output-dir", output_dir
    ]
    import subprocess
    res = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(f"API fetch process finished with exit code {res.returncode}")
    if res.stdout:
        logger.info(res.stdout[:500])
    if res.stderr:
        logger.warning(res.stderr[:500])
        
    json_files = list(Path(output_dir).glob("ashley_urls_*.json"))
    if not json_files:
        raise FileNotFoundError("No URL json files produced by run_ashley_scraper.py")
        
    latest_json = max(json_files, key=os.path.getmtime)
    logger.info(f"Reading collected URLs from API result JSON: {latest_json}")
    with open(latest_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    urls = data.get("urls", [])
    return urls

def fetch_from_direct(urls_str: str, urls_file: str = "") -> List[str]:
    urls = []
    if urls_file and os.path.exists(urls_file):
        logger.info(f"Reading direct URLs from file: {urls_file}")
        with open(urls_file, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u.startswith("http"):
                    urls.append(u)
    elif urls_str:
        logger.info("Parsing direct URLs from input text string...")
        for raw in urls_str.replace("\n", ",").split(","):
            u = raw.strip()
            if u.startswith("http"):
                urls.append(u)
    return urls

def main():
    parser = argparse.ArgumentParser(description="Fetch product URLs from FTP, Sitemap, API, or Direct inputs")
    parser.add_argument("--source-type", default="api", type=str.lower,
                        choices=["ftp", "sitemap", "api", "direct", "urls", "direct urls"],
                        help="Input source type for URL collection (ftp, sitemap, api, direct)")
    
    # FTP args
    parser.add_argument("--ftp-host", default="ftp-sandbox.1sb.pp.ua", help="FTP Hostname")
    parser.add_argument("--ftp-port", type=int, default=21, help="FTP Port")
    parser.add_argument("--ftp-user", default="onestop_ftp-sandbox", help="FTP Username")
    parser.add_argument("--ftp-pass", default="OneStop123", help="FTP Password")
    parser.add_argument("--ftp-path", default="/scrap/", help="FTP directory path")
    parser.add_argument("--ftp-filename", default="", help="Specific CSV filename on FTP server")
    
    # Sitemap args
    parser.add_argument("--sitemap-url", default="https://colemanfurniture.com/sitemap/sitemap_index.xml.gz", help="Sitemap XML URL")
    
    # API args
    parser.add_argument("--base-api", default="https://colemanfurniture.com/manufacturer/detail/250", help="Base API endpoint")
    parser.add_argument("--start-page", type=int, default=1, help="Start page")
    parser.add_argument("--end-page", type=int, default=150, help="End page")
    
    # Direct args
    parser.add_argument("--urls", default="", help="Comma or newline separated URLs")
    parser.add_argument("--urls-file", default="", help="Path to file containing URLs")
    
    # Output file
    parser.add_argument("--output-file", default="remaining_input/remaining_merged.csv", help="Target output CSV file path")
    parser.add_argument("--temp-dir", default="output", help="Directory for temporary files")
    
    args = parser.parse_args()
    
    source = args.source_type.lower()
    logger.info(f"=== Starting URL collection mode: {source.upper()} ===")
    
    urls = []
    if source == "ftp":
        urls = fetch_from_ftp(
            host=args.ftp_host,
            port=args.ftp_port,
            user=args.ftp_user,
            pass_=args.ftp_pass,
            remote_dir=args.ftp_path,
            target_filename=args.ftp_filename
        )
    elif source == "sitemap":
        urls = fetch_from_sitemap(sitemap_url=args.sitemap_url)
    elif source == "api":
        urls = fetch_from_api(
            base_api=args.base_api,
            start_page=args.start_page,
            end_page=args.end_page,
            output_dir=args.temp_dir
        )
    elif source in ["direct", "urls", "direct urls"]:
        urls = fetch_from_direct(urls_str=args.urls, urls_file=args.urls_file)
    else:
        raise ValueError(f"Unsupported source_type: {args.source_type}")
        
    if not urls:
        logger.error(f"No URLs collected using source_type '{source}'!")
        sys.exit(1)
        
    count = save_urls_to_csv(urls, args.output_file)
    
    # Save into standardized FTP structure
    try:
        root_dir = Path(__file__).resolve().parent.parent.parent
        if str(root_dir) not in sys.path:
            sys.path.insert(0, str(root_dir))
        from ftp_manager import FTPPathManager, save_sitemap_urls
        pm = FTPPathManager("Coleman")
        paths = pm.prepare_local_structure("output")
        save_urls_to_csv(urls, paths["input_file"])
        save_sitemap_urls(urls, paths["temp_url_list"])
        logger.info(f"✓ Standardized input path: {paths['input_file']}")
        logger.info(f"✓ Standardized temp sitemap path: {paths['temp_url_list']}")
    except Exception as exc:
        logger.warning(f"Note: Standardized FTP path resolution error: {exc}")

    logger.info(f"Finished processing. Total {count} URLs written to {args.output_file}")

if __name__ == "__main__":
    main()
