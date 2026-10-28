import requests
import xml.etree.ElementTree as ET
import gzip
from typing import List
from urllib.parse import urljoin
from .proxy_manager import ProxyManager
import logging
import time
import sys

logger = logging.getLogger(__name__)
class SitemapProcessor:
    
    def __init__(self):
        self.proxy_manager = None

    def _get_proxy_manager(self):
        if self.proxy_manager is None:
            self.proxy_manager = ProxyManager()
        return self.proxy_manager
    
    def _make_request_with_proxy(self, url: str, proxy: str = None, max_retries: int = 2) -> requests.Response:
        for attempt in range(max_retries):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                }
                
                proxies = None
                if proxy:
                    proxies = {"http": proxy, "https": proxy}
                    logger.debug(f"Attempt {attempt + 1} with proxy: {proxy}")
                
                response = requests.get(url, headers=headers, timeout=15, proxies=proxies)
                
                if response.status_code == 200:
                    return response
                elif response.status_code in [403, 429]:
                    logger.warning(f"Blocked with proxy {proxy}, status {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                
            except Exception as e:
                logger.debug(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
        
        raise Exception(f"Failed to fetch {url} after {max_retries} attempts")
    
    def get_sitemap_from_robots(self, site_url: str) -> str:
        site_url = site_url.rstrip('/')
        robots_url = urljoin(site_url + '/', 'robots.txt')
        
        logger.info(f"Checking robots.txt at: {robots_url}")
        
        use_proxy = 'homegallerystores.com' in site_url
        proxy = None
        
        if use_proxy:
            proxy_manager = self._get_proxy_manager()
            proxy = proxy_manager.get_proxy_for_homegallery()
            if proxy:
                logger.info(f"Using proxy for HomeGallery: {proxy}")
        
        try:
            response = self._make_request_with_proxy(robots_url, proxy)
            for line in response.text.split('\n'):
                line = line.strip()
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
                    return sitemap_url
        except Exception as e:
            logger.warning(f"Failed to get robots.txt with proxy: {e}")
            if use_proxy and proxy:
                try:
                    logger.info("Trying without proxy...")
                    response = self._make_request_with_proxy(robots_url, None)
                    for line in response.text.split('\n'):
                        line = line.strip()
                        if line.lower().startswith('sitemap:'):
                            sitemap_url = line.split(':', 1)[1].strip()
                            logger.info(f"Found sitemap in robots.txt (no proxy): {sitemap_url}")
                            return sitemap_url
                except Exception as e2:
                    logger.warning(f"Failed to get robots.txt without proxy: {e2}")
        
        common_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap/sitemap.xml',
            '/sitemap/sitemap_index.xml',
            '/sitemap.xml.gz',
            '/sitemap_index.xml.gz',
            '/sitemap/sitemap_index.xml.gz',
        ]
        
        logger.info("Trying common sitemap paths...")
        for path in common_paths:
            sitemap_url = urljoin(site_url + '/', path.lstrip('/'))
            try:
                response = self._make_request_with_proxy(sitemap_url, proxy)
                content_type = response.headers.get('content-type', '').lower()
                if any(x in content_type for x in ['xml', 'gzip', 'octet-stream']):
                    logger.info(f"Found sitemap at common path: {sitemap_url}")
                    return sitemap_url
            except Exception as e:
                logger.debug(f"Failed for {sitemap_url}: {e}")
                continue
        
        raise ValueError(f"No sitemap found for {site_url}")
    
    def extract_all_sitemaps(self, main_sitemap_url: str) -> List[str]:
        logger.info(f"Extracting sitemaps from: {main_sitemap_url}")
        
        use_proxy = 'homegallerystores.com' in main_sitemap_url
        proxy = None
        
        if use_proxy:
            proxy_manager = self._get_proxy_manager()
            proxy = proxy_manager.get_proxy_for_homegallery()
            if proxy:
                logger.info(f"Using proxy for HomeGallery sitemap extraction: {proxy}")
        
        try:
            if use_proxy and proxy:
                try:
                    response = self._make_request_with_proxy(main_sitemap_url, proxy)
                    return self._parse_sitemap_response(response, main_sitemap_url)
                except Exception as e:
                    logger.warning(f"Failed with proxy, trying without: {e}")
            
            response = self._make_request_with_proxy(main_sitemap_url, None)
            return self._parse_sitemap_response(response, main_sitemap_url)
            
        except Exception as e:
            logger.error(f"Failed to extract sitemaps from {main_sitemap_url}: {e}")
            raise Exception(f"Failed to parse sitemap {main_sitemap_url}: {e}")
    
    def _parse_sitemap_response(self, response: requests.Response, main_sitemap_url: str) -> List[str]:
        content = response.content
        
        try:
            if (main_sitemap_url.endswith('.gz') or 
                response.headers.get('content-encoding') == 'gzip'):
                content = gzip.decompress(content)
        except:
            pass
        
        root = ET.fromstring(content)
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        sitemaps = []
        for sitemap in root.findall('ns:sitemap/ns:loc', ns):
            if sitemap.text:
                sitemaps.append(sitemap.text.strip())
        
        if not sitemaps:
            for url in root.findall('ns:url/ns:loc', ns):
                if url.text:
                    sitemaps.append(url.text.strip())
            
            if not sitemaps:
                sitemaps = [main_sitemap_url]
        
        logger.info(f"Extracted {len(sitemaps)} sitemaps/URLs")
        return sitemaps
    
    @staticmethod
    def get_sitemap_chunks(all_sitemaps: List[str], offset: int, limit: int) -> List[str]:
        if not all_sitemaps:
            logger.warning("No sitemaps to process")
            return []
        
        if limit == 0:
            chunk = all_sitemaps[offset:]
        else:
            chunk = all_sitemaps[offset:offset + limit]
        
        logger.info(f"Returning chunk: offset={offset}, limit={limit}, size={len(chunk)}")
        return chunk
