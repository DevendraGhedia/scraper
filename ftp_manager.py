#!/usr/bin/env python3
"""
Centralized FTP Directory Structure & Sitemap URL Tracking Manager
Implements standard FTP path resolution, local folder structure initialization,
sitemap URL tracking comparison (Scraped vs Not Scraped), and FTP upload synchronization.

Standard FTP Structure:
{Competitor}/
└── {YYYYMMDD}/
    ├── input/
    │   └── URLList{timestamp}.csv
    │
    └── output/
        ├── temp/
        │   ├── ScrappedData{timestamp}.csv
        │   ├── URLListForScrap{timestamp}.csv
        │   └── [other temporary files]
        │
        ├── FinalScrappedData{timestamp}.csv
        └── FinalURLListForScrap{timestamp}.csv
"""

import os
import sys
import csv
import json
import logging
import argparse
import ftplib
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set, Optional, Tuple, Union
from urllib.parse import urlparse

# Configure logger
logger = logging.getLogger("ftp_manager")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def normalize_competitor_name(competitor: str) -> str:
    """Formats competitor key/name consistently (e.g. 'coleman' -> 'Coleman', 'cymax' -> 'Cymax')."""
    if not competitor:
        return "Unknown"
    comp = competitor.strip()
    # Preserve pre-formatted camelcase or titlecase
    if comp.istitle() or any(c.isupper() for c in comp[1:]):
        return comp
    words = comp.replace("-", " ").replace("_", " ").split()
    return " ".join(w.capitalize() for w in words)


def normalize_url(url: str) -> str:
    """Normalizes a URL for robust comparison (strip query params, trailing slashes, whitespace, lowercase domain)."""
    if not url:
        return ""
    u = str(url).strip()
    if not u:
        return ""
    try:
        parsed = urlparse(u)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path.rstrip("/")
        normalized = f"{scheme}://{netloc}{path}"
        if parsed.query:
            normalized += f"?{parsed.query}"
        return normalized
    except Exception:
        return u.rstrip("/").lower()


class FTPPathManager:
    """Centralized path resolution for input, temp, and final output directories and files."""

    def __init__(self, competitor: str, date_str: Optional[str] = None, timestamp: Optional[str] = None):
        self.competitor = normalize_competitor_name(competitor)
        self.date_str = date_str or datetime.now().strftime("%Y%m%d")
        self.timestamp = timestamp or datetime.now().strftime("%Y%m%d%H%M")

    # Relative directory paths
    def get_competitor_dir(self) -> str:
        return self.competitor

    def get_date_dir(self) -> str:
        return os.path.join(self.competitor, self.date_str)

    def get_input_dir(self) -> str:
        return os.path.join(self.competitor, self.date_str, "input")

    def get_output_dir(self) -> str:
        return os.path.join(self.competitor, self.date_str, "output")

    def get_temp_dir(self) -> str:
        return os.path.join(self.competitor, self.date_str, "output", "temp")

    # Standard File Paths
    def get_input_file_path(self, base_dir: str = "") -> str:
        filename = f"URLList{self.timestamp}.csv"
        rel_path = os.path.join(self.get_input_dir(), filename)
        return os.path.join(base_dir, rel_path) if base_dir else rel_path

    def get_temp_url_list_path(self, base_dir: str = "") -> str:
        filename = f"URLListForScrap{self.timestamp}.csv"
        rel_path = os.path.join(self.get_temp_dir(), filename)
        return os.path.join(base_dir, rel_path) if base_dir else rel_path

    def get_temp_scraped_data_path(self, base_dir: str = "") -> str:
        filename = f"ScrappedData{self.timestamp}.csv"
        rel_path = os.path.join(self.get_temp_dir(), filename)
        return os.path.join(base_dir, rel_path) if base_dir else rel_path

    def get_final_scraped_data_path(self, base_dir: str = "") -> str:
        filename = f"FinalScrappedData{self.timestamp}.csv"
        rel_path = os.path.join(self.get_output_dir(), filename)
        return os.path.join(base_dir, rel_path) if base_dir else rel_path

    def get_final_url_list_path(self, base_dir: str = "") -> str:
        filename = f"FinalURLListForScrap{self.timestamp}.csv"
        rel_path = os.path.join(self.get_output_dir(), filename)
        return os.path.join(base_dir, rel_path) if base_dir else rel_path

    def prepare_local_structure(self, base_dir: str = "output") -> Dict[str, str]:
        """Creates the local directory tree matching the required structure."""
        input_dir = os.path.join(base_dir, self.get_input_dir())
        temp_dir = os.path.join(base_dir, self.get_temp_dir())
        output_dir = os.path.join(base_dir, self.get_output_dir())

        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(temp_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        return {
            "competitor": self.competitor,
            "date_str": self.date_str,
            "timestamp": self.timestamp,
            "input_dir": input_dir,
            "temp_dir": temp_dir,
            "output_dir": output_dir,
            "input_file": self.get_input_file_path(base_dir),
            "temp_url_list": self.get_temp_url_list_path(base_dir),
            "temp_scraped_data": self.get_temp_scraped_data_path(base_dir),
            "final_scraped_data": self.get_final_scraped_data_path(base_dir),
            "final_url_list": self.get_final_url_list_path(base_dir),
        }


def extract_urls_from_file(file_path: str) -> List[str]:
    """Extracts all URLs from a CSV, JSON, or text file."""
    if not os.path.exists(file_path):
        logger.warning(f"File not found for URL extraction: {file_path}")
        return []

    urls = []
    file_lower = file_path.lower()

    if file_lower.endswith(".json"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                raw_urls = data.get("urls", []) or data.get("url_list", [])
            elif isinstance(data, list):
                raw_urls = data
            else:
                raw_urls = []
            for item in raw_urls:
                if isinstance(item, dict):
                    u = item.get("url") or item.get("Ref Product URL") or item.get("URL")
                else:
                    u = str(item)
                if u and str(u).strip().startswith("http"):
                    urls.append(str(u).strip())
        except Exception as e:
            logger.error(f"Error reading JSON file {file_path}: {e}")

    elif file_lower.endswith(".csv"):
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return []

                url_idx = -1
                for idx, col in enumerate(header):
                    col_clean = col.strip().lower()
                    if col_clean in ("url", "ref product url", "product url", "ref variant url", "canonical url"):
                        url_idx = idx
                        break
                if url_idx == -1:
                    url_idx = 0

                for row in reader:
                    if len(row) > url_idx:
                        u = row[url_idx].strip()
                        if u and u.startswith("http"):
                            urls.append(u)
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")

    else:
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    u = line.strip()
                    if u and u.startswith("http"):
                        urls.append(u)
        except Exception as e:
            logger.error(f"Error reading text file {file_path}: {e}")

    return urls


def save_sitemap_urls(urls: List[str], output_file_path: str) -> int:
    """Saves sitemap URLs into URLListForScrap{timestamp}.csv file."""
    out_dir = os.path.dirname(output_file_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    clean_urls = []
    seen = set()
    for u in urls:
        u_str = str(u).strip()
        if u_str and u_str.startswith("http") and u_str not in seen:
            seen.add(u_str)
            clean_urls.append(u_str)

    with open(output_file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["URL"])
        for u in clean_urls:
            writer.writerow([u])

    logger.info(f"Saved {len(clean_urls)} sitemap URLs to {output_file_path}")
    return len(clean_urls)


def generate_final_url_tracking(
    sitemap_urls_input: Union[str, List[str]],
    scraped_data_input: Union[str, List[str]],
    output_file_path: str
) -> Dict[str, Union[int, str]]:
    """
    Compares all sitemap URLs against scraped URLs and outputs FinalURLListForScrap{timestamp}.csv
    with columns: URL,Status (Scraped / Not Scraped).
    """
    if isinstance(sitemap_urls_input, str):
        sitemap_urls = extract_urls_from_file(sitemap_urls_input)
    else:
        sitemap_urls = list(sitemap_urls_input)

    if isinstance(scraped_data_input, str):
        scraped_urls = extract_urls_from_file(scraped_data_input)
    else:
        scraped_urls = list(scraped_data_input)

    scraped_normalized_set = {normalize_url(u) for u in scraped_urls if u}

    out_dir = os.path.dirname(output_file_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    total_sitemap = 0
    scraped_count = 0
    not_scraped_count = 0

    seen_urls = set()
    rows = []

    for raw_url in sitemap_urls:
        u_str = str(raw_url).strip()
        if not u_str or not u_str.startswith("http"):
            continue

        norm_u = normalize_url(u_str)
        if norm_u in seen_urls:
            continue
        seen_urls.add(norm_u)

        total_sitemap += 1
        is_scraped = norm_u in scraped_normalized_set
        status = "Scraped" if is_scraped else "Not Scraped"

        if is_scraped:
            scraped_count += 1
        else:
            not_scraped_count += 1

        rows.append([u_str, status])

    with open(output_file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Status"])
        writer.writerows(rows)

    logger.info(f"✓ Generated Final URL Tracking File: {output_file_path}")
    logger.info(f"Summary: Total Sitemap URLs: {total_sitemap} | Scraped: {scraped_count} | Not Scraped: {not_scraped_count}")

    return {
        "total": total_sitemap,
        "scraped": scraped_count,
        "not_scraped": not_scraped_count,
        "output_file": output_file_path
    }


class FTPManager:
    """Handles FTP upload synchronization matching the {Competitor}/{YYYYMMDD}/ structure."""

    def __init__(self, host: str, user: str, pass_: str, port: int = 21, base_dir: str = ""):
        self.host = host
        self.user = user
        self.pass_ = pass_
        self.port = port
        self.base_dir = base_dir.rstrip("/")
        self.ftp: Optional[ftplib.FTP] = None

    def connect(self):
        logger.info(f"Connecting to FTP {self.host}:{self.port} as user '{self.user}'...")
        self.ftp = ftplib.FTP()
        self.ftp.connect(self.host, self.port, timeout=60)
        self.ftp.login(self.user, self.pass_)
        if self.base_dir:
            self._makedirs(self.base_dir)
            self.ftp.cwd(self.base_dir)
            logger.info(f"FTP working directory set to: {self.base_dir}")

    def disconnect(self):
        if self.ftp:
            try:
                self.ftp.quit()
            except Exception:
                pass
            self.ftp = None

    def _makedirs(self, remote_dir: str):
        """Recursively creates remote FTP directory path if missing."""
        if not remote_dir or remote_dir == "/":
            return
        parts = [p for p in remote_dir.replace("\\", "/").split("/") if p]
        curr = ""
        for p in parts:
            curr += "/" + p
            try:
                self.ftp.cwd(curr)
            except Exception:
                try:
                    self.ftp.mkd(curr)
                    self.ftp.cwd(curr)
                except Exception as e:
                    logger.warning(f"Note creating FTP directory {curr}: {e}")

    def upload_competitor_tree(self, local_base_dir: str, competitor: str, date_str: Optional[str] = None):
        """Uploads the local competitor directory tree matching {Competitor}/{YYYYMMDD}/... to FTP."""
        pm = FTPPathManager(competitor, date_str=date_str)
        comp_dir_name = pm.competitor
        date_dir_name = pm.date_str

        # Search inside local_base_dir for competitor folder
        target_root = os.path.join(local_base_dir, comp_dir_name, date_dir_name)
        if not os.path.exists(target_root):
            target_root = local_base_dir

        if not self.ftp:
            self.connect()

        uploaded = 0
        for root, _, files in os.walk(target_root):
            for file in files:
                local_path = os.path.join(root, file)
                rel_path = os.path.relpath(local_path, local_base_dir)
                remote_path = f"{self.base_dir}/{rel_path}".replace("\\", "/").replace("//", "/")
                remote_dir = os.path.dirname(remote_path)

                self._makedirs(remote_dir)
                logger.info(f"Uploading '{file}' -> FTP '{remote_path}'...")
                with open(local_path, "rb") as f:
                    self.ftp.storbinary(f"STOR {file}", f)
                uploaded += 1

        logger.info(f"✓ Uploaded {uploaded} files to FTP for {pm.competitor}/{pm.date_str}.")
        return uploaded

    def upload_file(self, local_path: str, remote_file_path: str, max_retries: int = 3) -> bool:
        """Uploads a single local file directly to remote_file_path on the FTP server."""
        if not os.path.exists(local_path):
            logger.warning(f"Local file does not exist for upload: {local_path}")
            return False

        remote_clean = remote_file_path.replace("\\", "/").strip("/")
        remote_dir = os.path.dirname(remote_clean)
        remote_filename = os.path.basename(remote_clean)

        for attempt in range(max_retries):
            try:
                if not self.ftp:
                    self.connect()
                if remote_dir:
                    self._makedirs(remote_dir)
                    self.ftp.cwd(f"/{remote_dir}" if not remote_dir.startswith("/") else remote_dir)

                with open(local_path, "rb") as f:
                    self.ftp.storbinary(f"STOR {remote_filename}", f)
                logger.info(f"✓ Uploaded '{remote_filename}' -> FTP '/{remote_dir}/{remote_filename}'")
                return True
            except Exception as e:
                logger.warning(f"FTP upload attempt {attempt+1} failed for {remote_filename}: {e}")
                self.disconnect()
                time.sleep(2 * (attempt + 1))

        logger.error(f"Failed to upload {local_path} to FTP after {max_retries} attempts.")
        return False


def main():
    parser = argparse.ArgumentParser(description="Centralized FTP Storage & Sitemap URL Tracking CLI")
    subparsers = parser.add_subparsers(dest="command", help="Sub-command to run")

    # Command: init-dirs
    init_parser = subparsers.add_parser("init-dirs", help="Initialize local directory structure for a competitor")
    init_parser.add_argument("-c", "--competitor", required=True, help="Competitor name (e.g. Coleman)")
    init_parser.add_argument("-b", "--base-dir", default="output", help="Base output directory")
    init_parser.add_argument("-d", "--date", default="", help="Date string YYYYMMDD (default today)")

    # Command: track-urls
    track_parser = subparsers.add_parser("track-urls", help="Generate FinalURLListForScrap CSV comparing sitemap vs scraped data")
    track_parser.add_argument("-s", "--sitemap-file", required=True, help="Input sitemap URLs file (CSV/JSON/TXT)")
    track_parser.add_argument("-d", "--scraped-file", required=True, help="Scraped data file (CSV/JSON)")
    track_parser.add_argument("-o", "--output-file", required=True, help="Output FinalURLListForScrap CSV path")

    # Command: upload
    upload_parser = subparsers.add_parser("upload", help="Upload local competitor directory tree to FTP")
    upload_parser.add_argument("-c", "--competitor", required=True, help="Competitor name (e.g. Coleman)")
    upload_parser.add_argument("-b", "--base-dir", default="output", help="Local base output directory containing structure")
    upload_parser.add_argument("-d", "--date", default="", help="Date string YYYYMMDD")

    # Command: upload-file
    upload_file_parser = subparsers.add_parser("upload-file", help="Upload a single local file directly to a remote FTP path")
    upload_file_parser.add_argument("-l", "--local-file", required=True, help="Local file path")
    upload_file_parser.add_argument("-r", "--remote-file", required=True, help="Remote FTP file path")

    args = parser.parse_args()

    if args.command == "init-dirs":
        pm = FTPPathManager(args.competitor, date_str=args.date)
        paths = pm.prepare_local_structure(args.base_dir)
        print(json.dumps(paths, indent=2))

    elif args.command == "track-urls":
        res = generate_final_url_tracking(args.sitemap_file, args.scraped_file, args.output_file)
        print(json.dumps(res, indent=2))

    elif args.command == "upload":
        host = os.getenv("FTP_HOST", "")
        user = os.getenv("FTP_USER", "")
        pass_ = os.getenv("FTP_PASS", "")
        port = int(os.getenv("FTP_PORT", "21"))
        base_dir = os.getenv("FTP_PATH", os.getenv("FTP_BASE_DIR", ""))

        if not host or not user or not pass_:
            logger.error("Missing FTP_HOST, FTP_USER, or FTP_PASS environment variables.")
            sys.exit(1)

        ftp_mgr = FTPManager(host, user, pass_, port, base_dir)
        try:
            ftp_mgr.connect()
            ftp_mgr.upload_competitor_tree(args.base_dir, args.competitor, args.date)
        finally:
            ftp_mgr.disconnect()

    elif args.command == "upload-file":
        host = os.getenv("FTP_HOST", "")
        user = os.getenv("FTP_USER", "")
        pass_ = os.getenv("FTP_PASS", "")
        port = int(os.getenv("FTP_PORT", "21"))
        base_dir = os.getenv("FTP_PATH", os.getenv("FTP_BASE_DIR", ""))

        if not host or not user or not pass_:
            logger.error("Missing FTP_HOST, FTP_USER, or FTP_PASS environment variables.")
            sys.exit(1)

        ftp_mgr = FTPManager(host, user, pass_, port, base_dir)
        try:
            ftp_mgr.connect()
            success = ftp_mgr.upload_file(args.local_file, args.remote_file)
            sys.exit(0 if success else 1)
        finally:
            ftp_mgr.disconnect()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
