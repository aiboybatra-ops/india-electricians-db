"""
JustDial scraper for electricians in India.
JustDial is one of the largest local business directories in India.
"""
import re
import json
from typing import List, Optional
from urllib.parse import quote
from bs4 import BeautifulSoup

from src.scrapers import BaseScraper
from src.models import Electrician, ScrapeResult


class JustDialScraper(BaseScraper):
    """Scraper for JustDial.com"""
    
    def __init__(self):
        super().__init__("justdial")
        self.base_url = "https://www.justdial.com"
    
    def _decode_justdial_phone(self, encoded_classes: List[str]) -> str:
        """
        JustDial encodes phone numbers using CSS classes.
        This decodes them back to actual digits.
        """
        # JustDial uses class names like 'acb', 'dc', etc. to represent digits
        # The mapping changes periodically, so this is a common pattern
        class_to_digit = {
            'acb': '0', 'ij': '1', 'dc': '2', 'fe': '3', 'hg': '4',
            'lk': '5', 'nm': '6', 'po': '7', 'rq': '8', 'ts': '9',
            # Alternative mappings
            'icon-ji': '1', 'icon-dc': '2', 'icon-fe': '3', 'icon-hg': '4',
            'icon-lk': '5', 'icon-nm': '6', 'icon-po': '7', 'icon-rq': '8',
            'icon-ts': '9', 'icon-acb': '0',
        }
        
        phone = ""
        for cls in encoded_classes:
            for class_name, digit in class_to_digit.items():
                if class_name in cls:
                    phone += digit
                    break
        
        return phone if len(phone) >= 10 else ""
    
    def _build_search_url(self, city: str, category: str = "electricians", page: int = 1) -> str:
        """Build JustDial search URL."""
        city_slug = city.lower().replace(" ", "-")
        
        if page == 1:
            return f"{self.base_url}/{city_slug}/{category}"
        else:
            return f"{self.base_url}/{city_slug}/{category}/page-{page}"
    
    def _parse_listing(self, listing_div, city: str, state: str) -> Optional[Electrician]:
        """Parse a single listing from JustDial."""
        try:
            # Extract name
            name_elem = listing_div.find("span", class_="lng_cont_name") or \
                       listing_div.find("a", class_="store-name")
            name = name_elem.get_text(strip=True) if name_elem else None
            
            if not name:
                name_elem = listing_div.find("h2") or listing_div.find("h3")
                name = name_elem.get_text(strip=True) if name_elem else "Unknown"
            
            # Extract phone number
            phone = None
            
            # Method 1: Look for encoded phone spans
            phone_container = listing_div.find("div", class_="contact-info") or \
                            listing_div.find("p", class_="contact-info")
            
            if phone_container:
                phone_spans = phone_container.find_all("span")
                if phone_spans:
                    classes = [span.get("class", []) for span in phone_spans]
                    flat_classes = [c for sublist in classes for c in (sublist if isinstance(sublist, list) else [sublist])]
                    phone = self._decode_justdial_phone(flat_classes)
            
            # Method 2: Look for data attributes
            if not phone:
                phone_elem = listing_div.find(attrs={"data-phone": True})
                if phone_elem:
                    phone = phone_elem.get("data-phone", "")
            
            # Method 3: Extract from text
            if not phone:
                text = listing_div.get_text()
                phones = self._extract_phone_numbers(text)
                if phones:
                    phone = phones[0]
            
            if not phone:
                return None
            
            # Extract address
            address = None
            address_elem = listing_div.find("span", class_="cont_fl_addr") or \
                          listing_div.find("p", class_="address-info")
            if address_elem:
                address = self._clean_text(address_elem.get_text())
            
            # Extract rating
            rating = None
            rating_elem = listing_div.find("span", class_="green-box") or \
                         listing_div.find("span", class_="rating")
            if rating_elem:
                try:
                    rating = float(rating_elem.get_text(strip=True))
                except (ValueError, TypeError):
                    pass
            
            # Extract review count
            review_count = None
            reviews_elem = listing_div.find("span", class_="rt_count") or \
                          listing_div.find("a", class_="reviews")
            if reviews_elem:
                review_text = reviews_elem.get_text()
                numbers = re.findall(r'\d+', review_text)
                if numbers:
                    review_count = int(numbers[0])
            
            # Extract listing URL
            source_url = None
            link_elem = listing_div.find("a", href=True)
            if link_elem:
                href = link_elem.get("href", "")
                if href.startswith("/"):
                    source_url = f"{self.base_url}{href}"
                elif href.startswith("http"):
                    source_url = href
            
            return Electrician(
                name=self._clean_text(name),
                phone=phone,
                city=city,
                state=state,
                address=address,
                rating=rating,
                review_count=review_count,
                source="justdial",
                source_url=source_url,
            )
            
        except Exception as e:
            self.logger.debug(f"Error parsing listing: {e}")
            return None
    
    def _scrape_page(self, url: str, city: str, state: str) -> List[Electrician]:
        """Scrape a single page of JustDial results."""
        electricians = []
        
        try:
            headers = {
                "Referer": "https://www.justdial.com/",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            
            response = self._make_request(url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            
            # Find all listing containers
            listing_selectors = [
                "li.cntanr",
                "div.store-details",
                "div.resultbox_info",
                "div.jsx-business-card",
                "section.jd-business-card",
            ]
            
            listings = []
            for selector in listing_selectors:
                if "." in selector:
                    tag, class_name = selector.split(".", 1)
                    listings = soup.find_all(tag or "div", class_=class_name)
                    if listings:
                        break
            
            # If no specific selectors work, try generic approach
            if not listings:
                # Look for any div containing phone-like patterns
                all_divs = soup.find_all("div")
                for div in all_divs:
                    text = div.get_text()
                    if re.search(r'[6-9]\d{9}', text) and \
                       any(word in text.lower() for word in ["electrician", "electric", "wiring"]):
                        listings.append(div)
            
            for listing in listings:
                electrician = self._parse_listing(listing, city, state)
                if electrician:
                    electricians.append(electrician)
            
            self.logger.debug(f"Found {len(electricians)} electricians on page")
            
        except Exception as e:
            self.logger.error(f"Error scraping page {url}: {e}")
        
        return electricians
    
    def scrape_city(self, city: str, state: str, max_pages: int = 5) -> ScrapeResult:
        """Scrape electrician data for a specific city from JustDial."""
        all_electricians = []
        pages_scraped = 0
        
        try:
            categories = ["electricians", "electrical-contractors", "electrical-lineman"]
            
            for category in categories:
                for page in range(1, max_pages + 1):
                    url = self._build_search_url(city, category, page)
                    self.logger.info(f"Scraping {url}")
                    
                    electricians = self._scrape_page(url, city, state)
                    pages_scraped += 1
                    
                    if not electricians:
                        self.logger.debug(f"No results on page {page}, stopping")
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
            self.logger.error(f"Error scraping JustDial for {city}, {state}: {e}")
            return ScrapeResult(
                success=False,
                source=self.name,
                city=city,
                state=state,
                error_message=str(e),
                pages_scraped=pages_scraped,
            )
