"""
Sulekha scraper for electricians in India.
Sulekha is a popular local services marketplace in India.
"""
import re
import json
from typing import List, Optional
from urllib.parse import quote
from bs4 import BeautifulSoup

from src.scrapers import BaseScraper
from src.models import Electrician, ScrapeResult


class SulekhaScraper(BaseScraper):
    """Scraper for Sulekha.com"""
    
    def __init__(self):
        super().__init__("sulekha")
        self.base_url = "https://www.sulekha.com"
    
    def _build_search_url(self, city: str, category: str = "electricians", page: int = 1) -> str:
        """Build Sulekha search URL."""
        city_slug = city.lower().replace(" ", "-")
        
        if page == 1:
            return f"{self.base_url}/{category}/{city_slug}"
        else:
            return f"{self.base_url}/{category}/{city_slug}?page={page}"
    
    def _parse_listing(self, listing_div, city: str, state: str) -> Optional[Electrician]:
        """Parse a single Sulekha listing."""
        try:
            # Extract name
            name = None
            name_selectors = [
                ("h2", "vendor-name"),
                ("a", "store-name"),
                ("span", "title"),
                ("h3", None),
            ]
            
            for tag, class_name in name_selectors:
                if class_name:
                    name_elem = listing_div.find(tag, class_=class_name)
                else:
                    name_elem = listing_div.find(tag)
                if name_elem:
                    name = name_elem.get_text(strip=True)
                    break
            
            if not name:
                return None
            
            # Extract phone number
            phone = None
            
            # Method 1: Look for phone in data attributes
            phone_elem = listing_div.find(attrs={"data-phone": True})
            if phone_elem:
                phone = phone_elem.get("data-phone", "")
            
            # Method 2: Look for mobile button or link
            if not phone:
                mobile_btn = listing_div.find("a", href=re.compile(r"tel:"))
                if mobile_btn:
                    href = mobile_btn.get("href", "")
                    phone_match = re.search(r'\d+', href)
                    if phone_match:
                        phone = phone_match.group()
            
            # Method 3: Look for phone in class elements
            if not phone:
                phone_container = listing_div.find("span", class_=re.compile(r"phone|mobile|contact"))
                if phone_container:
                    phones = self._extract_phone_numbers(phone_container.get_text())
                    if phones:
                        phone = phones[0]
            
            # Method 4: Extract from any visible text
            if not phone:
                text = listing_div.get_text()
                phones = self._extract_phone_numbers(text)
                if phones:
                    phone = phones[0]
            
            if not phone:
                return None
            
            # Extract address
            address = None
            address_elem = listing_div.find("span", class_=re.compile(r"address|location|area"))
            if address_elem:
                address = self._clean_text(address_elem.get_text())
            
            # Extract rating
            rating = None
            rating_elem = listing_div.find("span", class_=re.compile(r"rating|star"))
            if rating_elem:
                try:
                    rating_text = rating_elem.get_text()
                    rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                    if rating_match:
                        rating = float(rating_match.group(1))
                except (ValueError, TypeError):
                    pass
            
            # Extract review count
            review_count = None
            reviews_elem = listing_div.find("span", class_=re.compile(r"review|feedback"))
            if reviews_elem:
                review_text = reviews_elem.get_text()
                numbers = re.findall(r'\d+', review_text)
                if numbers:
                    review_count = int(numbers[0])
            
            # Extract experience
            experience = None
            exp_elem = listing_div.find(string=re.compile(r'\d+\s*(years?|yrs?)', re.I))
            if exp_elem:
                exp_match = re.search(r'(\d+)\s*(years?|yrs?)', str(exp_elem), re.I)
                if exp_match:
                    experience = int(exp_match.group(1))
            
            # Extract source URL
            source_url = None
            link_elem = listing_div.find("a", href=True)
            if link_elem:
                href = link_elem.get("href", "")
                if href.startswith("/"):
                    source_url = f"{self.base_url}{href}"
                elif href.startswith("http"):
                    source_url = href
            
            # Extract services
            services = []
            service_container = listing_div.find("div", class_=re.compile(r"service|skill"))
            if service_container:
                service_items = service_container.find_all("span")
                services = [self._clean_text(s.get_text()) for s in service_items if s.get_text().strip()]
            
            return Electrician(
                name=self._clean_text(name),
                phone=phone,
                city=city,
                state=state,
                address=address,
                rating=rating,
                review_count=review_count,
                experience_years=experience,
                services=services[:5],  # Limit to 5 services
                source="sulekha",
                source_url=source_url,
            )
            
        except Exception as e:
            self.logger.debug(f"Error parsing Sulekha listing: {e}")
            return None
    
    def _scrape_page(self, url: str, city: str, state: str) -> List[Electrician]:
        """Scrape a single page of Sulekha results."""
        electricians = []
        
        try:
            headers = {
                "Referer": "https://www.sulekha.com/",
            }
            
            response = self._make_request(url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            
            # Try to extract from JSON-LD
            scripts = soup.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and data.get("@type") == "LocalBusiness":
                        phone = data.get("telephone", "")
                        if phone:
                            phones = self._extract_phone_numbers(str(phone))
                            if phones:
                                electricians.append(
                                    Electrician(
                                        name=data.get("name", "Unknown"),
                                        phone=phones[0],
                                        city=city,
                                        state=state,
                                        address=data.get("address", {}).get("streetAddress"),
                                        rating=data.get("aggregateRating", {}).get("ratingValue"),
                                        review_count=data.get("aggregateRating", {}).get("reviewCount"),
                                        source="sulekha",
                                    )
                                )
                except json.JSONDecodeError:
                    pass
            
            # Find listing containers
            listing_selectors = [
                "div.vendor-card",
                "div.listing-card",
                "div.service-provider",
                "li.vendor-item",
                "div.result-card",
            ]
            
            listings = []
            for selector in listing_selectors:
                if "." in selector:
                    tag, class_name = selector.split(".", 1)
                    listings = soup.find_all(tag or "div", class_=class_name)
                    if listings:
                        break
            
            # Fallback: look for any container with phone numbers
            if not listings:
                all_containers = soup.find_all(["div", "li", "article"])
                for container in all_containers:
                    text = container.get_text()
                    if self._extract_phone_numbers(text) and \
                       any(kw in text.lower() for kw in ["electrician", "electric", "wiring"]):
                        listings.append(container)
            
            for listing in listings:
                electrician = self._parse_listing(listing, city, state)
                if electrician:
                    electricians.append(electrician)
            
            self.logger.debug(f"Found {len(electricians)} electricians on page")
            
        except Exception as e:
            self.logger.error(f"Error scraping Sulekha page {url}: {e}")
        
        return electricians
    
    def scrape_city(self, city: str, state: str, max_pages: int = 3) -> ScrapeResult:
        """Scrape electrician data for a specific city from Sulekha."""
        all_electricians = []
        pages_scraped = 0
        
        try:
            categories = [
                "electricians",
                "electrical-contractors",
                "electrical-repair-services",
                "home-electrical-services",
            ]
            
            for category in categories:
                for page in range(1, max_pages + 1):
                    url = self._build_search_url(city, category, page)
                    self.logger.info(f"Scraping {url}")
                    
                    electricians = self._scrape_page(url, city, state)
                    pages_scraped += 1
                    
                    if not electricians:
                        break
                    
                    all_electricians.extend(electricians)
            
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
            self.logger.error(f"Error scraping Sulekha for {city}, {state}: {e}")
            return ScrapeResult(
                success=False,
                source=self.name,
                city=city,
                state=state,
                error_message=str(e),
                pages_scraped=pages_scraped,
            )
