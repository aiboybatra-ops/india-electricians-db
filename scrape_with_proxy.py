#!/usr/bin/env python3
"""
Production scraper with proxy support.
Use this for real scraping with rotating proxies.
"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
from bs4 import BeautifulSoup
import re
import time
import random
import logging
from dotenv import load_dotenv

from src.models import Electrician
from src.storage import DataStorage
from src.proxy_manager import ProxyManager, ProxyProviderManager, Proxy

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize
storage = DataStorage()
proxy_provider = ProxyProviderManager()

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
]


def setup_proxies() -> ProxyManager:
    """Configure proxy manager based on environment variables."""
    
    # Check for BrightData
    if os.getenv("BRIGHTDATA_CUSTOMER_ID"):
        logger.info("Using BrightData proxies")
        return proxy_provider.setup_brightdata(
            customer_id=os.getenv("BRIGHTDATA_CUSTOMER_ID"),
            zone=os.getenv("BRIGHTDATA_ZONE", "residential"),
            password=os.getenv("BRIGHTDATA_PASSWORD"),
        )
    
    # Check for ScraperAPI
    if os.getenv("SCRAPERAPI_KEY"):
        logger.info("Using ScraperAPI proxies")
        return proxy_provider.setup_scraperapi(os.getenv("SCRAPERAPI_KEY"))
    
    # Check for Oxylabs
    if os.getenv("OXYLABS_USERNAME"):
        logger.info("Using Oxylabs proxies")
        return proxy_provider.setup_oxylabs(
            username=os.getenv("OXYLABS_USERNAME"),
            password=os.getenv("OXYLABS_PASSWORD"),
        )
    
    # Check for proxy list in env
    if os.getenv("PROXY_LIST"):
        logger.info("Using proxy list from environment")
        return proxy_provider.load_from_env()
    
    # Check for proxy file
    proxy_file = os.getenv("PROXY_FILE", "proxies.txt")
    if os.path.exists(proxy_file):
        logger.info(f"Loading proxies from {proxy_file}")
        return proxy_provider.load_from_file(proxy_file)
    
    # Check for single proxy
    if os.getenv("PROXY_HOST"):
        logger.info("Using single proxy from environment")
        return proxy_provider.load_from_env()
    
    # Use free proxies as fallback
    if os.getenv("USE_FREE_PROXIES", "false").lower() == "true":
        logger.info("Loading free proxies (may be slow/unreliable)")
        count = int(os.getenv("FREE_PROXY_COUNT", 20))
        return proxy_provider.load_free_proxies(count)
    
    logger.warning("No proxy configuration found. Scraping without proxies may get blocked.")
    return ProxyManager()


def get_headers():
    """Get randomized headers."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


def extract_phone_numbers(text):
    """Extract Indian phone numbers from text."""
    patterns = [
        r'\+91[\s\-]?[6-9]\d{9}',
        r'91[\s\-]?[6-9]\d{9}',
        r'0[6-9]\d{9}',
        r'[6-9]\d{9}',
        r'[6-9]\d{4}[\s\-]?\d{5}',
    ]
    
    phones = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        phones.extend(matches)
    
    normalized = []
    for phone in phones:
        digits = "".join(filter(str.isdigit, phone))
        if len(digits) >= 10:
            normalized.append(digits[-10:])
    
    return list(set(normalized))


def make_request(url: str, proxy_manager: ProxyManager, max_retries: int = 3):
    """Make a request with proxy rotation and retry logic."""
    
    for attempt in range(max_retries):
        proxy = proxy_manager.get_proxy() if proxy_manager.count > 0 else None
        proxies = proxy.dict if proxy else None
        
        try:
            # Add delay between requests
            delay = random.uniform(
                float(os.getenv("REQUEST_DELAY_MIN", 3)),
                float(os.getenv("REQUEST_DELAY_MAX", 7))
            )
            time.sleep(delay)
            
            response = requests.get(
                url,
                headers=get_headers(),
                proxies=proxies,
                timeout=30,
            )
            
            # Check for blocks
            if response.status_code == 403 or "captcha" in response.text.lower():
                logger.warning(f"Blocked on {url}, rotating proxy...")
                if proxy:
                    proxy_manager.mark_failure(proxy)
                continue
            
            if response.status_code == 200:
                if proxy:
                    proxy_manager.mark_success(proxy)
                return response
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed: {e}")
            if proxy:
                proxy_manager.mark_failure(proxy)
    
    return None


def scrape_justdial(city: str, state: str, proxy_manager: ProxyManager):
    """Scrape JustDial with proxy support."""
    electricians = []
    city_slug = city.lower().replace(" ", "-")
    
    urls = [
        f"https://www.justdial.com/{city_slug}/electricians",
        f"https://www.justdial.com/{city_slug}/electrical-contractors",
    ]
    
    for url in urls:
        logger.info(f"Scraping: {url}")
        response = make_request(url, proxy_manager)
        
        if not response:
            logger.warning(f"Failed to fetch {url}")
            continue
        
        soup = BeautifulSoup(response.text, "lxml")
        
        # Find all potential listing containers
        for div in soup.find_all(["div", "li", "section"], class_=re.compile(r"cntanr|store|result|jsx|card")):
            text = div.get_text()
            phones = extract_phone_numbers(text)
            
            if phones:
                # Try to find name
                name_elem = div.find(["h2", "h3", "span", "a"], class_=re.compile(r"name|title|lng_cont|store"))
                name = name_elem.get_text(strip=True)[:100] if name_elem else "Electrician"
                
                # Try to find address
                addr_elem = div.find(["span", "p", "div"], class_=re.compile(r"addr|location|area"))
                address = addr_elem.get_text(strip=True)[:200] if addr_elem else None
                
                # Try to find rating
                rating = None
                rating_elem = div.find(["span"], class_=re.compile(r"rating|green-box|star"))
                if rating_elem:
                    try:
                        rating = float(re.search(r'(\d+\.?\d*)', rating_elem.get_text()).group(1))
                    except:
                        pass
                
                for phone in phones[:1]:
                    electricians.append(Electrician(
                        name=name,
                        phone=phone,
                        city=city,
                        state=state,
                        address=address,
                        rating=rating,
                        source="justdial",
                        source_url=url,
                    ))
    
    return electricians


def scrape_sulekha(city: str, state: str, proxy_manager: ProxyManager):
    """Scrape Sulekha with proxy support."""
    electricians = []
    city_slug = city.lower().replace(" ", "-")
    
    url = f"https://www.sulekha.com/electricians/{city_slug}"
    logger.info(f"Scraping: {url}")
    
    response = make_request(url, proxy_manager)
    
    if not response:
        logger.warning(f"Failed to fetch {url}")
        return electricians
    
    soup = BeautifulSoup(response.text, "lxml")
    
    for div in soup.find_all(["div", "article", "section"], class_=re.compile(r"vendor|card|listing|provider|result")):
        text = div.get_text()
        phones = extract_phone_numbers(text)
        
        if phones:
            name_elem = div.find(["h2", "h3", "a", "span"], class_=re.compile(r"name|title|vendor"))
            name = name_elem.get_text(strip=True)[:100] if name_elem else "Electrician"
            
            for phone in phones[:1]:
                electricians.append(Electrician(
                    name=name,
                    phone=phone,
                    city=city,
                    state=state,
                    source="sulekha",
                    source_url=url,
                ))
    
    return electricians


def scrape_city(city: str, state: str, proxy_manager: ProxyManager):
    """Scrape all sources for a city."""
    all_electricians = []
    
    logger.info(f"\n{'='*50}")
    logger.info(f"📍 Scraping {city}, {state}")
    logger.info(f"{'='*50}")
    
    # Scrape JustDial
    try:
        results = scrape_justdial(city, state, proxy_manager)
        all_electricians.extend(results)
        logger.info(f"JustDial: Found {len(results)} listings")
    except Exception as e:
        logger.error(f"JustDial error: {e}")
    
    # Scrape Sulekha
    try:
        results = scrape_sulekha(city, state, proxy_manager)
        all_electricians.extend(results)
        logger.info(f"Sulekha: Found {len(results)} listings")
    except Exception as e:
        logger.error(f"Sulekha error: {e}")
    
    # Deduplicate and save
    if all_electricians:
        unique = list({e.get_unique_key(): e for e in all_electricians}.values())
        saved = storage.save_to_database(unique)
        logger.info(f"✅ Saved {saved} new records for {city}")
        return saved
    
    return 0


def main():
    """Main scraping function."""
    print("\n" + "="*60)
    print("🔌 India Electricians Scraper - Production Mode")
    print("="*60)
    
    # Setup proxy manager
    proxy_manager = setup_proxies()
    
    if proxy_manager.count > 0:
        print(f"\n✅ Loaded {proxy_manager.count} proxies")
        
        # Test proxies
        print("🔍 Testing proxies...")
        stats = proxy_manager.test_all_proxies()
        print(f"   Working: {stats['working']}, Failed: {stats['failed']}")
        
        if proxy_manager.count == 0:
            print("❌ No working proxies! Exiting.")
            return
    else:
        print("\n⚠️  No proxies configured. Scraping may get blocked.")
        print("   Configure proxies in .env file or add proxies.txt")
        response = input("\n   Continue without proxies? (y/n): ")
        if response.lower() != 'y':
            return
    
    # Cities to scrape
    cities = [
        ("Delhi", "Delhi"),
        ("Mumbai", "Maharashtra"),
        ("Bangalore", "Karnataka"),
        ("Chennai", "Tamil Nadu"),
        ("Hyderabad", "Telangana"),
        ("Kolkata", "West Bengal"),
        ("Pune", "Maharashtra"),
        ("Ahmedabad", "Gujarat"),
        ("Jaipur", "Rajasthan"),
        ("Lucknow", "Uttar Pradesh"),
        ("Chandigarh", "Chandigarh"),
        ("Kochi", "Kerala"),
        ("Indore", "Madhya Pradesh"),
        ("Bhopal", "Madhya Pradesh"),
        ("Nagpur", "Maharashtra"),
    ]
    
    total_saved = 0
    
    for city, state in cities:
        try:
            saved = scrape_city(city, state, proxy_manager)
            total_saved += saved
        except KeyboardInterrupt:
            print("\n\n⚠️  Scraping interrupted by user")
            break
        except Exception as e:
            logger.error(f"Error scraping {city}: {e}")
    
    # Final stats
    print("\n" + "="*60)
    print("📊 SCRAPING COMPLETE")
    print("="*60)
    
    stats = storage.get_statistics()
    print(f"\n✅ Total records: {stats['total_records']}")
    print(f"📝 New records added: {total_saved}")
    
    if proxy_manager.count > 0:
        proxy_stats = proxy_manager.get_stats()
        print(f"\n🔄 Proxy Stats:")
        print(f"   Total proxies: {proxy_stats['total']}")
        print(f"   Avg success rate: {proxy_stats['avg_success_rate']:.1%}")
    
    print(f"\n🎉 View data at http://localhost:5000")


if __name__ == "__main__":
    main()
