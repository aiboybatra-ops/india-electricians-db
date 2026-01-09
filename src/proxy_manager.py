"""
Proxy Manager for rotating proxies during web scraping.
Supports multiple proxy providers and rotation strategies.
"""
import os
import random
import time
import requests
from typing import List, Optional, Dict
from dataclasses import dataclass
from collections import deque
import logging

logger = logging.getLogger(__name__)


@dataclass
class Proxy:
    """Represents a proxy server."""
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = "http"
    country: Optional[str] = None
    last_used: float = 0
    fail_count: int = 0
    success_count: int = 0
    
    @property
    def url(self) -> str:
        """Get proxy URL."""
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"
    
    @property
    def dict(self) -> Dict[str, str]:
        """Get proxy dict for requests."""
        return {
            "http": self.url,
            "https": self.url,
        }
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.success_count + self.fail_count
        return self.success_count / total if total > 0 else 0.5


class ProxyManager:
    """
    Manages a pool of proxies with rotation and health checking.
    """
    
    def __init__(
        self,
        proxies: List[Proxy] = None,
        rotation_strategy: str = "round_robin",  # round_robin, random, weighted
        min_delay_between_uses: float = 5.0,
        max_failures: int = 5,
    ):
        self.proxies = deque(proxies or [])
        self.rotation_strategy = rotation_strategy
        self.min_delay_between_uses = min_delay_between_uses
        self.max_failures = max_failures
        self._current_index = 0
    
    def add_proxy(self, proxy: Proxy):
        """Add a proxy to the pool."""
        self.proxies.append(proxy)
    
    def add_proxies_from_list(self, proxy_list: List[str]):
        """
        Add proxies from a list of strings.
        Format: host:port or host:port:username:password or protocol://host:port
        """
        for proxy_str in proxy_list:
            try:
                proxy = self._parse_proxy_string(proxy_str)
                if proxy:
                    self.proxies.append(proxy)
            except Exception as e:
                logger.warning(f"Failed to parse proxy {proxy_str}: {e}")
    
    def _parse_proxy_string(self, proxy_str: str) -> Optional[Proxy]:
        """Parse a proxy string into a Proxy object."""
        proxy_str = proxy_str.strip()
        if not proxy_str:
            return None
        
        protocol = "http"
        if "://" in proxy_str:
            protocol, proxy_str = proxy_str.split("://", 1)
        
        # Handle user:pass@host:port format
        if "@" in proxy_str:
            auth, host_port = proxy_str.rsplit("@", 1)
            username, password = auth.split(":", 1)
            host, port = host_port.split(":")
            return Proxy(
                host=host,
                port=int(port),
                username=username,
                password=password,
                protocol=protocol,
            )
        
        # Handle host:port:user:pass format
        parts = proxy_str.split(":")
        if len(parts) == 2:
            return Proxy(host=parts[0], port=int(parts[1]), protocol=protocol)
        elif len(parts) == 4:
            return Proxy(
                host=parts[0],
                port=int(parts[1]),
                username=parts[2],
                password=parts[3],
                protocol=protocol,
            )
        
        return None
    
    def get_proxy(self) -> Optional[Proxy]:
        """Get the next proxy based on rotation strategy."""
        if not self.proxies:
            return None
        
        # Remove failed proxies
        self.proxies = deque([p for p in self.proxies if p.fail_count < self.max_failures])
        
        if not self.proxies:
            logger.error("All proxies have failed!")
            return None
        
        if self.rotation_strategy == "round_robin":
            proxy = self.proxies[0]
            self.proxies.rotate(-1)
        elif self.rotation_strategy == "random":
            proxy = random.choice(list(self.proxies))
        elif self.rotation_strategy == "weighted":
            # Prefer proxies with higher success rates
            weights = [p.success_rate + 0.1 for p in self.proxies]
            proxy = random.choices(list(self.proxies), weights=weights)[0]
        else:
            proxy = self.proxies[0]
        
        # Ensure minimum delay between uses
        time_since_last = time.time() - proxy.last_used
        if time_since_last < self.min_delay_between_uses:
            time.sleep(self.min_delay_between_uses - time_since_last)
        
        proxy.last_used = time.time()
        return proxy
    
    def mark_success(self, proxy: Proxy):
        """Mark a proxy request as successful."""
        proxy.success_count += 1
        proxy.fail_count = max(0, proxy.fail_count - 1)  # Reduce fail count on success
    
    def mark_failure(self, proxy: Proxy):
        """Mark a proxy request as failed."""
        proxy.fail_count += 1
        logger.warning(f"Proxy {proxy.host}:{proxy.port} failed ({proxy.fail_count}/{self.max_failures})")
    
    def test_proxy(self, proxy: Proxy, timeout: int = 10) -> bool:
        """Test if a proxy is working."""
        test_urls = [
            "https://httpbin.org/ip",
            "https://api.ipify.org?format=json",
        ]
        
        for url in test_urls:
            try:
                response = requests.get(
                    url,
                    proxies=proxy.dict,
                    timeout=timeout,
                )
                if response.status_code == 200:
                    logger.info(f"Proxy {proxy.host}:{proxy.port} is working")
                    return True
            except Exception:
                continue
        
        logger.warning(f"Proxy {proxy.host}:{proxy.port} failed test")
        return False
    
    def test_all_proxies(self) -> Dict[str, int]:
        """Test all proxies and return stats."""
        working = 0
        failed = 0
        
        for proxy in list(self.proxies):
            if self.test_proxy(proxy):
                working += 1
            else:
                failed += 1
                self.proxies.remove(proxy)
        
        return {"working": working, "failed": failed}
    
    @property
    def count(self) -> int:
        """Get number of available proxies."""
        return len(self.proxies)
    
    def get_stats(self) -> Dict:
        """Get proxy pool statistics."""
        if not self.proxies:
            return {"total": 0, "avg_success_rate": 0}
        
        return {
            "total": len(self.proxies),
            "avg_success_rate": sum(p.success_rate for p in self.proxies) / len(self.proxies),
            "total_requests": sum(p.success_count + p.fail_count for p in self.proxies),
        }


class ProxyProviderManager:
    """
    Manages proxies from various providers.
    Supports: BrightData, ScraperAPI, ProxyScrape (free), WebShare, etc.
    """
    
    def __init__(self):
        self.proxy_manager = ProxyManager()
    
    def load_from_env(self):
        """Load proxy configuration from environment variables."""
        # Single proxy from env
        host = os.getenv("PROXY_HOST")
        port = os.getenv("PROXY_PORT")
        
        if host and port:
            proxy = Proxy(
                host=host,
                port=int(port),
                username=os.getenv("PROXY_USERNAME"),
                password=os.getenv("PROXY_PASSWORD"),
            )
            self.proxy_manager.add_proxy(proxy)
            logger.info(f"Loaded proxy from env: {host}:{port}")
        
        # Proxy list from env (comma-separated)
        proxy_list = os.getenv("PROXY_LIST", "")
        if proxy_list:
            self.proxy_manager.add_proxies_from_list(proxy_list.split(","))
            logger.info(f"Loaded {self.proxy_manager.count} proxies from PROXY_LIST")
        
        return self.proxy_manager
    
    def load_from_file(self, filepath: str):
        """Load proxies from a file (one per line)."""
        try:
            with open(filepath, "r") as f:
                proxy_list = [line.strip() for line in f if line.strip() and not line.startswith("#")]
            self.proxy_manager.add_proxies_from_list(proxy_list)
            logger.info(f"Loaded {len(proxy_list)} proxies from {filepath}")
        except FileNotFoundError:
            logger.warning(f"Proxy file not found: {filepath}")
        
        return self.proxy_manager
    
    def load_free_proxies(self, count: int = 20) -> ProxyManager:
        """
        Load free proxies from public sources.
        Note: Free proxies are unreliable and slow. Use paid proxies for production.
        """
        free_proxy_sources = [
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        ]
        
        proxies_found = []
        
        for source in free_proxy_sources:
            try:
                response = requests.get(source, timeout=10)
                if response.status_code == 200:
                    lines = response.text.strip().split("\n")
                    for line in lines[:count]:
                        line = line.strip()
                        if ":" in line:
                            proxies_found.append(line.split()[0])  # Handle "ip:port country" format
                
                if len(proxies_found) >= count:
                    break
                    
            except Exception as e:
                logger.debug(f"Failed to fetch from {source}: {e}")
        
        self.proxy_manager.add_proxies_from_list(proxies_found[:count])
        logger.info(f"Loaded {len(proxies_found[:count])} free proxies")
        
        return self.proxy_manager
    
    def setup_brightdata(self, customer_id: str, zone: str, password: str):
        """
        Set up BrightData (formerly Luminati) rotating proxies.
        Get credentials at: https://brightdata.com/
        """
        # BrightData rotating residential proxy
        proxy = Proxy(
            host="brd.superproxy.io",
            port=22225,
            username=f"brd-customer-{customer_id}-zone-{zone}",
            password=password,
        )
        self.proxy_manager.add_proxy(proxy)
        logger.info("Configured BrightData rotating proxy")
        return self.proxy_manager
    
    def setup_scraperapi(self, api_key: str):
        """
        Set up ScraperAPI proxy.
        Get API key at: https://www.scraperapi.com/
        """
        # ScraperAPI uses a different approach - prepend to URL
        # But we can also use their proxy mode
        proxy = Proxy(
            host="proxy-server.scraperapi.com",
            port=8001,
            username="scraperapi",
            password=api_key,
        )
        self.proxy_manager.add_proxy(proxy)
        logger.info("Configured ScraperAPI proxy")
        return self.proxy_manager
    
    def setup_webshare(self, proxies: List[str]):
        """
        Set up WebShare.io proxies.
        Format: ip:port:username:password
        Get proxies at: https://www.webshare.io/
        """
        self.proxy_manager.add_proxies_from_list(proxies)
        logger.info(f"Configured {len(proxies)} WebShare proxies")
        return self.proxy_manager
    
    def setup_oxylabs(self, username: str, password: str):
        """
        Set up Oxylabs rotating residential proxies.
        Get credentials at: https://oxylabs.io/
        """
        proxy = Proxy(
            host="pr.oxylabs.io",
            port=7777,
            username=username,
            password=password,
        )
        self.proxy_manager.add_proxy(proxy)
        logger.info("Configured Oxylabs rotating proxy")
        return self.proxy_manager


# Convenience function
def create_proxy_manager_from_env() -> ProxyManager:
    """Create a ProxyManager with configuration from environment."""
    provider = ProxyProviderManager()
    return provider.load_from_env()
