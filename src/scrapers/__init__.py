"""
Base scraper class with common functionality.
"""
import random
import time
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import (
    REQUEST_DELAY_MIN,
    REQUEST_DELAY_MAX,
    MAX_RETRIES,
    USER_AGENTS,
    PROXY_CONFIG,
)
from src.models import Electrician, ScrapeResult


class BaseScraper(ABC):
    """Base class for all scrapers with common functionality."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = self._setup_logger()
        self.session = self._create_session()
        self._request_count = 0
        
        # Try to use fake_useragent, fallback to predefined list
        try:
            self.ua = UserAgent()
            self._use_fake_ua = True
        except Exception:
            self._use_fake_ua = False
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logger for the scraper."""
        logger = logging.getLogger(f"scraper.{self.name}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f"%(asctime)s - {self.name} - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set up proxy if configured
        if PROXY_CONFIG.get("host") and PROXY_CONFIG.get("port"):
            proxy_url = self._build_proxy_url()
            session.proxies = {
                "http": proxy_url,
                "https": proxy_url,
            }
        
        return session
    
    def _build_proxy_url(self) -> str:
        """Build proxy URL from configuration."""
        host = PROXY_CONFIG["host"]
        port = PROXY_CONFIG["port"]
        username = PROXY_CONFIG.get("username", "")
        password = PROXY_CONFIG.get("password", "")
        
        if username and password:
            return f"http://{username}:{password}@{host}:{port}"
        return f"http://{host}:{port}"
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent string."""
        if self._use_fake_ua:
            try:
                return self.ua.random
            except Exception:
                pass
        return random.choice(USER_AGENTS)
    
    def _get_headers(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Get request headers with random user agent."""
        headers = {
            "User-Agent": self._get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
        }
        if extra_headers:
            headers.update(extra_headers)
        return headers
    
    def _random_delay(self, min_delay: float = None, max_delay: float = None):
        """Add a random delay between requests."""
        min_d = min_delay or REQUEST_DELAY_MIN
        max_d = max_delay or REQUEST_DELAY_MAX
        delay = random.uniform(min_d, max_d)
        self.logger.debug(f"Sleeping for {delay:.2f} seconds")
        time.sleep(delay)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _make_request(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        timeout: int = 30,
    ) -> requests.Response:
        """Make an HTTP request with retry logic."""
        self._request_count += 1
        
        # Add delay between requests
        if self._request_count > 1:
            self._random_delay()
        
        request_headers = self._get_headers(headers)
        
        self.logger.debug(f"Making {method} request to {url}")
        
        try:
            if method.upper() == "GET":
                response = self.session.get(
                    url,
                    params=params,
                    headers=request_headers,
                    timeout=timeout,
                )
            elif method.upper() == "POST":
                response = self.session.post(
                    url,
                    params=params,
                    data=data,
                    headers=request_headers,
                    timeout=timeout,
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            raise
    
    def _extract_phone_numbers(self, text: str) -> List[str]:
        """Extract Indian phone numbers from text."""
        import re
        
        # Patterns for Indian phone numbers
        patterns = [
            r'\+91[\s\-]?[6-9]\d{9}',  # +91 format
            r'91[\s\-]?[6-9]\d{9}',     # 91 format
            r'0[6-9]\d{9}',              # 0 prefix
            r'[6-9]\d{9}',               # Plain 10 digit
            r'[6-9]\d{4}[\s\-]?\d{5}',   # With space/dash in middle
        ]
        
        phones = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)
        
        # Normalize phone numbers
        normalized = []
        for phone in phones:
            # Remove all non-digit characters
            digits = "".join(filter(str.isdigit, phone))
            # Get last 10 digits
            if len(digits) >= 10:
                normalized.append(digits[-10:])
        
        return list(set(normalized))
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        # Remove extra whitespace
        text = " ".join(text.split())
        # Remove special characters but keep basic punctuation
        text = text.strip()
        return text
    
    @abstractmethod
    def scrape_city(self, city: str, state: str) -> ScrapeResult:
        """
        Scrape electrician data for a specific city.
        Must be implemented by subclasses.
        """
        pass
    
    def scrape_locations(self, locations: Dict[str, List[str]]) -> List[ScrapeResult]:
        """Scrape multiple locations."""
        results = []
        
        for state, cities in locations.items():
            for city in cities:
                self.logger.info(f"Scraping {city}, {state}")
                try:
                    result = self.scrape_city(city, state)
                    results.append(result)
                    self.logger.info(
                        f"Found {len(result.electricians)} electricians in {city}, {state}"
                    )
                except Exception as e:
                    self.logger.error(f"Error scraping {city}, {state}: {e}")
                    results.append(
                        ScrapeResult(
                            success=False,
                            source=self.name,
                            city=city,
                            state=state,
                            error_message=str(e),
                        )
                    )
        
        return results
    
    def close(self):
        """Close the session and clean up resources."""
        if self.session:
            self.session.close()
