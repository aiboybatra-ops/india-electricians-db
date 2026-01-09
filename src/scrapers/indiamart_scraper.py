"""
IndiaMART scraper for electricians and electrical contractors.
IndiaMART is a major B2B marketplace in India with business listings.
"""
import re
import json
from typing import List, Optional
from urllib.parse import quote, urlencode
from bs4 import BeautifulSoup

from src.scrapers import BaseScraper
from src.models import Electrician, ScrapeResult


class IndiaMARTScraper(BaseScraper):
    """Scraper for IndiaMART.com"""
    
    def __init__(self):
        super().__init__("indiamart")
        self.base_url = "https://dir.indiamart.com"
        self.search_url = "https://www.indiamart.com/search.mp"
    
    def _build_search_url(self, keyword: str, city: str, page: int = 1) -> str:
        """Build IndiaMART search URL."""
        params = {
            "ss": f"{keyword} {city}",
            "cq": city,
            "mcatid": "",
            "catid": "",
            "gclid": "",
            "prdsrc": "1",
            "res": "RC4",
        }
        
        if page > 1:
            params["biz"] = str((page - 1) * 25)
        
        return f"{self.search_url}?{urlencode(params)}"
    
    def _parse_listing(self, listing_div, city: str, state: str) -> Optional[Electrician]:
        """Parse a single IndiaMART listing."""
        try:
            # Extract company/business name
            name = None
            name_elem = listing_div.find("a", class_="pnm") or \
                       listing_div.find("span", class_="company-name") or \
                       listing_div.find("h2")
            
            if name_elem:
                name = name_elem.get_text(strip=True)
            
            if not name:
                return None
            
            # Extract phone number
            phone = None
            
            # Method 1: Look for mobile/phone in data attributes
            phone_elem = listing_div.find(attrs={"data-mobile": True})
            if phone_elem:
                phone = phone_elem.get("data-mobile", "")
            
            # Method 2: Look for phone in class elements
            if not phone:
                phone_container = listing_div.find("div", class_="phn") or \
                                 listing_div.find("span", class_="phone-number")
                if phone_container:
                    phone_text = phone_container.get_text()
                    phones = self._extract_phone_numbers(phone_text)
                    if phones:
                        phone = phones[0]
            
            # Method 3: Extract from any visible text
            if not phone:
                text = listing_div.get_text()
                phones = self._extract_phone_numbers(text)
                if phones:
                    phone = phones[0]
            
            if not phone:
                return None
            
            # Extract address
            address = None
            address_elem = listing_div.find("p", class_="lnk") or \
                          listing_div.find("span", class_="address") or \
                          listing_div.find("div", class_="location")
            
            if address_elem:
                address = self._clean_text(address_elem.get_text())
            
            # Extract website
            website = None
            website_elem = listing_div.find("a", href=re.compile(r"^https?://"))
            if website_elem:
                website = website_elem.get("href")
            
            # Extract source URL
            source_url = None
            link_elem = listing_div.find("a", class_="pnm") or listing_div.find("a", href=True)
            if link_elem:
                href = link_elem.get("href", "")
                if href.startswith("http"):
                    source_url = href
                elif href.startswith("/"):
                    source_url = f"https://www.indiamart.com{href}"
            
            # Extract services from listing text
            services = []
            text_content = listing_div.get_text().lower()
            service_keywords = [
                "house wiring", "industrial electrical", "commercial electrical",
                "electrical repair", "panel installation", "generator",
                "solar", "ac installation", "maintenance"
            ]
            for service in service_keywords:
                if service in text_content:
                    services.append(service.title())
            
            return Electrician(
                name=self._clean_text(name),
                phone=phone,
                city=city,
                state=state,
                address=address,
                website=website,
                services=services,
                source="indiamart",
                source_url=source_url,
            )
            
        except Exception as e:
            self.logger.debug(f"Error parsing IndiaMART listing: {e}")
            return None
    
    def _scrape_directory_page(self, city: str, state: str) -> List[Electrician]:
        """Scrape IndiaMART directory pages."""
        electricians = []
        city_slug = city.lower().replace(" ", "-")
        
        directory_urls = [
            f"{self.base_url}/{city_slug}/electricians.html",
            f"{self.base_url}/{city_slug}/electrical-contractors.html",
            f"{self.base_url}/{city_slug}/electrical-services.html",
        ]
        
        for url in directory_urls:
            try:
                response = self._make_request(url)
                soup = BeautifulSoup(response.text, "lxml")
                
                # Find listing containers
                listings = soup.find_all("div", class_=re.compile(r"lst|card|result"))
                
                for listing in listings:
                    electrician = self._parse_listing(listing, city, state)
                    if electrician:
                        electricians.append(electrician)
                        
            except Exception as e:
                self.logger.debug(f"Error scraping {url}: {e}")
        
        return electricians
    
    def _scrape_search_page(self, url: str, city: str, state: str) -> List[Electrician]:
        """Scrape a search results page."""
        electricians = []
        
        try:
            headers = {
                "Referer": "https://www.indiamart.com/",
            }
            
            response = self._make_request(url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            
            # Try to extract data from JSON in script tags
            scripts = soup.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        for item in data:
                            if item.get("@type") == "LocalBusiness":
                                phone = item.get("telephone", "")
                                if phone:
                                    phones = self._extract_phone_numbers(str(phone))
                                    if phones:
                                        electricians.append(
                                            Electrician(
                                                name=item.get("name", "Unknown"),
                                                phone=phones[0],
                                                city=city,
                                                state=state,
                                                address=item.get("address", {}).get("streetAddress"),
                                                website=item.get("url"),
                                                source="indiamart",
                                            )
                                        )
                except json.JSONDecodeError:
                    pass
            
            # Also parse HTML listings
            listing_containers = soup.find_all("div", class_=re.compile(r"card|listing|result|company"))
            for listing in listing_containers:
                electrician = self._parse_listing(listing, city, state)
                if electrician:
                    electricians.append(electrician)
            
        except Exception as e:
            self.logger.error(f"Error scraping search page {url}: {e}")
        
        return electricians
    
    def scrape_city(self, city: str, state: str, max_pages: int = 3) -> ScrapeResult:
        """Scrape electrician data for a specific city from IndiaMART."""
        all_electricians = []
        pages_scraped = 0
        
        try:
            # First try directory pages
            self.logger.info(f"Scraping IndiaMART directory for {city}, {state}")
            directory_results = self._scrape_directory_page(city, state)
            all_electricians.extend(directory_results)
            pages_scraped += 1
            
            # Then try search pages
            keywords = ["electrician", "electrical contractor", "electrical services"]
            
            for keyword in keywords:
                for page in range(1, max_pages + 1):
                    url = self._build_search_url(keyword, city, page)
                    self.logger.debug(f"Scraping {url}")
                    
                    results = self._scrape_search_page(url, city, state)
                    pages_scraped += 1
                    
                    if not results:
                        break
                    
                    all_electricians.extend(results)
            
            # Remove duplicates
            unique_electricians = list(
                {e.get_unique_key(): e for e in all_electricians}.values()
            )
            
            return ScrapeResult(
                success=True,
                source=self.name,
                city=city,
                state=state,
                electricians=unique_electricians,
                pages_scraped=pages_scraped,
            )
            
        except Exception as e:
            self.logger.error(f"Error scraping IndiaMART for {city}, {state}: {e}")
            return ScrapeResult(
                success=False,
                source=self.name,
                city=city,
                state=state,
                error_message=str(e),
                pages_scraped=pages_scraped,
            )
