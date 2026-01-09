"""
Urban Company (formerly UrbanClap) scraper for electricians.
Note: Urban Company has strong anti-scraping measures, so this uses their search API patterns.
"""
import re
import json
from typing import List, Optional
from bs4 import BeautifulSoup

from src.scrapers import BaseScraper
from src.models import Electrician, ScrapeResult


class UrbanCompanyScraper(BaseScraper):
    """Scraper for UrbanCompany.com (formerly UrbanClap)"""
    
    def __init__(self):
        super().__init__("urban_company")
        self.base_url = "https://www.urbancompany.com"
    
    def _get_city_url(self, city: str) -> str:
        """Get the city-specific URL."""
        city_slug = city.lower().replace(" ", "-")
        return f"{self.base_url}/{city_slug}"
    
    def _build_service_url(self, city: str, service: str = "electricians") -> str:
        """Build service URL for a city."""
        city_slug = city.lower().replace(" ", "-")
        return f"{self.base_url}/{city_slug}/{service}"
    
    def _parse_provider(self, provider_data: dict, city: str, state: str) -> Optional[Electrician]:
        """Parse provider data from Urban Company."""
        try:
            name = provider_data.get("name") or provider_data.get("display_name")
            if not name:
                return None
            
            # Urban Company typically doesn't expose phone numbers directly
            # They use their platform for booking
            phone = provider_data.get("phone") or provider_data.get("mobile")
            
            if not phone:
                # Try to extract from contact info
                contact = provider_data.get("contact", {})
                phone = contact.get("phone") or contact.get("mobile")
            
            if not phone:
                return None
            
            rating = None
            if "rating" in provider_data:
                try:
                    rating = float(provider_data["rating"])
                except (ValueError, TypeError):
                    pass
            
            review_count = provider_data.get("review_count") or provider_data.get("ratings_count")
            
            experience = None
            if "experience" in provider_data:
                try:
                    exp_text = str(provider_data["experience"])
                    exp_match = re.search(r'(\d+)', exp_text)
                    if exp_match:
                        experience = int(exp_match.group(1))
                except (ValueError, TypeError):
                    pass
            
            services = provider_data.get("services", [])
            if isinstance(services, list):
                services = [s.get("name", s) if isinstance(s, dict) else str(s) for s in services[:5]]
            else:
                services = []
            
            return Electrician(
                name=name,
                phone=str(phone),
                city=city,
                state=state,
                rating=rating,
                review_count=review_count,
                experience_years=experience,
                services=services,
                source="urban_company",
                verified=provider_data.get("verified", False),
            )
            
        except Exception as e:
            self.logger.debug(f"Error parsing Urban Company provider: {e}")
            return None
    
    def _scrape_page(self, url: str, city: str, state: str) -> List[Electrician]:
        """Scrape a single page from Urban Company."""
        electricians = []
        
        try:
            headers = {
                "Referer": f"{self.base_url}/",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "X-Requested-With": "XMLHttpRequest",
            }
            
            response = self._make_request(url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            
            # Look for provider data in scripts
            scripts = soup.find_all("script")
            for script in scripts:
                if script.string and ("providers" in script.string or "professionals" in script.string):
                    try:
                        # Try to extract JSON data
                        json_match = re.search(r'(\{[^{}]*"providers"[^{}]*\}|\[[^\[\]]*\])', script.string)
                        if json_match:
                            data = json.loads(json_match.group())
                            providers = data if isinstance(data, list) else data.get("providers", [])
                            for provider in providers:
                                electrician = self._parse_provider(provider, city, state)
                                if electrician:
                                    electricians.append(electrician)
                    except (json.JSONDecodeError, AttributeError):
                        pass
            
            # Also try to parse HTML listings
            provider_cards = soup.find_all("div", class_=re.compile(r"provider|professional|card"))
            for card in provider_cards:
                text = card.get_text()
                phones = self._extract_phone_numbers(text)
                
                if phones:
                    name_elem = card.find(["h2", "h3", "span"], class_=re.compile(r"name|title"))
                    name = name_elem.get_text(strip=True) if name_elem else "Unknown"
                    
                    electricians.append(
                        Electrician(
                            name=name,
                            phone=phones[0],
                            city=city,
                            state=state,
                            source="urban_company",
                        )
                    )
            
        except Exception as e:
            self.logger.error(f"Error scraping Urban Company page {url}: {e}")
        
        return electricians
    
    def scrape_city(self, city: str, state: str) -> ScrapeResult:
        """Scrape electrician data for a specific city from Urban Company."""
        all_electricians = []
        pages_scraped = 0
        
        try:
            services = [
                "electricians",
                "electrical-services",
                "ac-electrician",
                "home-electrician",
            ]
            
            for service in services:
                url = self._build_service_url(city, service)
                self.logger.info(f"Scraping {url}")
                
                electricians = self._scrape_page(url, city, state)
                pages_scraped += 1
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
            self.logger.error(f"Error scraping Urban Company for {city}, {state}: {e}")
            return ScrapeResult(
                success=False,
                source=self.name,
                city=city,
                state=state,
                error_message=str(e),
                pages_scraped=pages_scraped,
            )
