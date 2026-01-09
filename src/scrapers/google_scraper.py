"""
Google Maps/Places scraper for electricians.
Supports both API-based and web scraping approaches.
"""
import json
import re
import urllib.parse
from typing import List, Optional
from bs4 import BeautifulSoup

from src.scrapers import BaseScraper
from src.models import Electrician, ScrapeResult
from src.config import GOOGLE_PLACES_API_KEY, SEARCH_KEYWORDS


class GoogleMapsScraper(BaseScraper):
    """Scraper for Google Maps/Places."""
    
    def __init__(self):
        super().__init__("google_maps")
        self.base_url = "https://www.google.com/maps/search/"
        self.places_api_url = "https://maps.googleapis.com/maps/api/place"
        self._use_api = bool(GOOGLE_PLACES_API_KEY)
    
    def _search_places_api(
        self,
        query: str,
        location: str,
        radius: int = 50000,
    ) -> List[dict]:
        """
        Search using Google Places API.
        Requires valid API key.
        """
        if not GOOGLE_PLACES_API_KEY:
            self.logger.warning("No Google Places API key configured")
            return []
        
        results = []
        next_page_token = None
        
        while True:
            params = {
                "key": GOOGLE_PLACES_API_KEY,
                "query": f"{query} in {location}",
                "type": "electrician",
            }
            
            if next_page_token:
                params["pagetoken"] = next_page_token
            
            try:
                response = self._make_request(
                    f"{self.places_api_url}/textsearch/json",
                    params=params,
                )
                data = response.json()
                
                if data.get("status") != "OK":
                    self.logger.warning(f"API returned status: {data.get('status')}")
                    break
                
                results.extend(data.get("results", []))
                
                next_page_token = data.get("next_page_token")
                if not next_page_token:
                    break
                    
            except Exception as e:
                self.logger.error(f"Places API error: {e}")
                break
        
        return results
    
    def _get_place_details(self, place_id: str) -> dict:
        """Get detailed information about a place."""
        if not GOOGLE_PLACES_API_KEY:
            return {}
        
        params = {
            "key": GOOGLE_PLACES_API_KEY,
            "place_id": place_id,
            "fields": "name,formatted_phone_number,international_phone_number,"
                     "formatted_address,website,rating,user_ratings_total,"
                     "opening_hours,business_status",
        }
        
        try:
            response = self._make_request(
                f"{self.places_api_url}/details/json",
                params=params,
            )
            data = response.json()
            return data.get("result", {})
        except Exception as e:
            self.logger.error(f"Error getting place details: {e}")
            return {}
    
    def _scrape_google_search(self, query: str, city: str, state: str) -> List[Electrician]:
        """
        Scrape Google search results for electricians.
        This is a fallback when API is not available.
        """
        electricians = []
        search_query = f"{query} in {city} {state} India phone number contact"
        encoded_query = urllib.parse.quote(search_query)
        
        url = f"https://www.google.com/search?q={encoded_query}&num=100"
        
        try:
            response = self._make_request(
                url,
                headers={
                    "Accept": "text/html,application/xhtml+xml",
                }
            )
            
            soup = BeautifulSoup(response.text, "lxml")
            
            # Extract phone numbers from search results
            text_content = soup.get_text()
            phone_numbers = self._extract_phone_numbers(text_content)
            
            # Try to find business listings in the results
            for div in soup.find_all("div", class_=re.compile(r".*")):
                text = div.get_text()
                
                # Look for patterns that indicate a business listing
                if any(keyword in text.lower() for keyword in ["electrician", "electrical", "wiring"]):
                    phones = self._extract_phone_numbers(text)
                    if phones:
                        # Try to extract name from the same div
                        name = self._extract_business_name(text)
                        for phone in phones:
                            electricians.append(
                                Electrician(
                                    name=name or "Unknown",
                                    phone=phone,
                                    city=city,
                                    state=state,
                                    source="google_search",
                                )
                            )
            
        except Exception as e:
            self.logger.error(f"Google search scraping error: {e}")
        
        return electricians
    
    def _extract_business_name(self, text: str) -> Optional[str]:
        """Try to extract business name from text."""
        # Look for common patterns
        lines = text.split('\n')
        for line in lines[:5]:  # Check first few lines
            line = line.strip()
            if len(line) > 3 and len(line) < 100:
                # Skip if it's just a phone number or address
                if not re.match(r'^[\d\s\-\+]+$', line):
                    return self._clean_text(line)
        return None
    
    def scrape_city(self, city: str, state: str) -> ScrapeResult:
        """Scrape electrician data for a specific city."""
        all_electricians = []
        pages_scraped = 0
        
        try:
            if self._use_api:
                self.logger.info(f"Using Google Places API for {city}, {state}")
                
                for keyword in SEARCH_KEYWORDS[:3]:  # Use top 3 keywords
                    places = self._search_places_api(keyword, f"{city}, {state}, India")
                    
                    for place in places:
                        details = self._get_place_details(place.get("place_id", ""))
                        
                        phone = details.get("formatted_phone_number") or details.get("international_phone_number")
                        if not phone:
                            continue
                        
                        electrician = Electrician(
                            name=place.get("name", "Unknown"),
                            phone=phone,
                            city=city,
                            state=state,
                            address=details.get("formatted_address"),
                            website=details.get("website"),
                            rating=details.get("rating"),
                            review_count=details.get("user_ratings_total"),
                            source="google_places_api",
                            source_url=f"https://www.google.com/maps/place/?q=place_id:{place.get('place_id')}",
                        )
                        all_electricians.append(electrician)
                    
                    pages_scraped += 1
            else:
                self.logger.info(f"Using web scraping for {city}, {state}")
                
                for keyword in SEARCH_KEYWORDS[:2]:
                    electricians = self._scrape_google_search(keyword, city, state)
                    all_electricians.extend(electricians)
                    pages_scraped += 1
            
            # Remove duplicates
            unique_electricians = list({e.get_unique_key(): e for e in all_electricians}.values())
            
            return ScrapeResult(
                success=True,
                source=self.name,
                city=city,
                state=state,
                electricians=unique_electricians,
                pages_scraped=pages_scraped,
            )
            
        except Exception as e:
            self.logger.error(f"Error scraping {city}, {state}: {e}")
            return ScrapeResult(
                success=False,
                source=self.name,
                city=city,
                state=state,
                error_message=str(e),
                pages_scraped=pages_scraped,
            )


class SerpApiGoogleScraper(BaseScraper):
    """
    Alternative Google scraper using SerpAPI.
    More reliable but requires paid API key.
    """
    
    def __init__(self, api_key: str):
        super().__init__("serpapi_google")
        self.api_key = api_key
        self.base_url = "https://serpapi.com/search"
    
    def scrape_city(self, city: str, state: str) -> ScrapeResult:
        """Scrape using SerpAPI."""
        all_electricians = []
        pages_scraped = 0
        
        try:
            for keyword in SEARCH_KEYWORDS[:3]:
                params = {
                    "engine": "google_maps",
                    "q": f"{keyword} in {city}, {state}, India",
                    "api_key": self.api_key,
                    "type": "search",
                    "hl": "en",
                }
                
                response = self._make_request(self.base_url, params=params)
                data = response.json()
                
                for result in data.get("local_results", []):
                    phone = result.get("phone")
                    if not phone:
                        continue
                    
                    electrician = Electrician(
                        name=result.get("title", "Unknown"),
                        phone=phone,
                        city=city,
                        state=state,
                        address=result.get("address"),
                        rating=result.get("rating"),
                        review_count=result.get("reviews"),
                        source="serpapi_google_maps",
                        source_url=result.get("link"),
                    )
                    all_electricians.append(electrician)
                
                pages_scraped += 1
            
            unique = list({e.get_unique_key(): e for e in all_electricians}.values())
            
            return ScrapeResult(
                success=True,
                source=self.name,
                city=city,
                state=state,
                electricians=unique,
                pages_scraped=pages_scraped,
            )
            
        except Exception as e:
            return ScrapeResult(
                success=False,
                source=self.name,
                city=city,
                state=state,
                error_message=str(e),
                pages_scraped=pages_scraped,
            )
