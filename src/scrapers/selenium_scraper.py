"""
Selenium-based scraper for websites with JavaScript rendering.
Use this when BeautifulSoup-based scrapers fail to get data.
"""
import time
import random
from typing import List, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

try:
    import undetected_chromedriver as uc
    HAS_UNDETECTED = True
except ImportError:
    HAS_UNDETECTED = False

from src.scrapers import BaseScraper
from src.models import Electrician, ScrapeResult
from src.config import USER_AGENTS


class SeleniumScraper(BaseScraper):
    """Base Selenium scraper for JavaScript-heavy websites."""
    
    def __init__(self, name: str, headless: bool = True, use_undetected: bool = True):
        super().__init__(name)
        self.headless = headless
        self.use_undetected = use_undetected and HAS_UNDETECTED
        self.driver = None
    
    def _create_driver(self):
        """Create and configure the Chrome WebDriver."""
        if self.use_undetected:
            options = uc.ChromeOptions()
            if self.headless:
                options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
            
            self.driver = uc.Chrome(options=options)
        else:
            options = Options()
            if self.headless:
                options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            
            # Remove webdriver flag
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
        
        self.driver.set_page_load_timeout(30)
        return self.driver
    
    def _ensure_driver(self):
        """Ensure driver is created."""
        if not self.driver:
            self._create_driver()
        return self.driver
    
    def _get_page(self, url: str, wait_for: str = None, timeout: int = 10) -> str:
        """Navigate to page and optionally wait for an element."""
        driver = self._ensure_driver()
        
        self._random_delay()
        driver.get(url)
        
        if wait_for:
            try:
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_for))
                )
            except TimeoutException:
                self.logger.warning(f"Timeout waiting for {wait_for}")
        
        # Random scroll to simulate human behavior
        self._random_scroll()
        
        return driver.page_source
    
    def _random_scroll(self):
        """Perform random scroll to simulate human behavior."""
        if self.driver:
            scroll_amount = random.randint(300, 800)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(0.5, 1.5))
    
    def _find_elements(self, selector: str, by: By = By.CSS_SELECTOR) -> list:
        """Find elements with error handling."""
        try:
            return self.driver.find_elements(by, selector)
        except NoSuchElementException:
            return []
    
    def _find_element(self, selector: str, by: By = By.CSS_SELECTOR):
        """Find single element with error handling."""
        try:
            return self.driver.find_element(by, selector)
        except NoSuchElementException:
            return None
    
    def scrape_city(self, city: str, state: str) -> ScrapeResult:
        """Override in subclass."""
        raise NotImplementedError
    
    def close(self):
        """Close the browser and clean up."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
        super().close()


class JustDialSeleniumScraper(SeleniumScraper):
    """Selenium-based scraper for JustDial - more reliable than HTTP requests."""
    
    def __init__(self, headless: bool = True):
        super().__init__("justdial_selenium", headless)
        self.base_url = "https://www.justdial.com"
    
    def _build_url(self, city: str, category: str = "Electricians") -> str:
        city_slug = city.lower().replace(" ", "-")
        return f"{self.base_url}/{city_slug}/{category}"
    
    def _extract_listings(self, city: str, state: str) -> List[Electrician]:
        """Extract listings from current page."""
        electricians = []
        
        # Wait for listings to load
        time.sleep(2)
        
        # Find all listing cards
        listings = self._find_elements(".cntanr, .store-details, .resultbox_info")
        
        for listing in listings:
            try:
                # Get name
                name_elem = listing.find_elements(By.CSS_SELECTOR, ".lng_cont_name, .store-name, h2, h3")
                name = name_elem[0].text if name_elem else "Unknown"
                
                # Get phone - JustDial shows phone on hover/click
                # Try to find the phone element
                phone = None
                
                # Try data attribute
                phone_elems = listing.find_elements(By.CSS_SELECTOR, "[data-phone]")
                if phone_elems:
                    phone = phone_elems[0].get_attribute("data-phone")
                
                # Try the contact number element
                if not phone:
                    contact_elems = listing.find_elements(By.CSS_SELECTOR, ".contact-info span, .mobilesv")
                    if contact_elems:
                        # Get all span classes for decoding
                        phone_text = "".join([e.text for e in contact_elems])
                        phones = self._extract_phone_numbers(phone_text)
                        if phones:
                            phone = phones[0]
                
                # Try clicking to reveal phone
                if not phone:
                    try:
                        show_btn = listing.find_elements(By.CSS_SELECTOR, ".callcontent, .show-more-mob")
                        if show_btn:
                            show_btn[0].click()
                            time.sleep(0.5)
                            phone_elems = listing.find_elements(By.CSS_SELECTOR, ".mobilesv span, .telnumcls")
                            if phone_elems:
                                phone_text = "".join([e.text for e in phone_elems])
                                phones = self._extract_phone_numbers(phone_text)
                                if phones:
                                    phone = phones[0]
                    except Exception:
                        pass
                
                if not phone:
                    continue
                
                # Get address
                address = None
                addr_elems = listing.find_elements(By.CSS_SELECTOR, ".cont_fl_addr, .address-info")
                if addr_elems:
                    address = addr_elems[0].text
                
                # Get rating
                rating = None
                rating_elems = listing.find_elements(By.CSS_SELECTOR, ".green-box, .rating")
                if rating_elems:
                    try:
                        rating = float(rating_elems[0].text)
                    except ValueError:
                        pass
                
                # Get source URL
                source_url = None
                link_elems = listing.find_elements(By.CSS_SELECTOR, "a[href]")
                if link_elems:
                    href = link_elems[0].get_attribute("href")
                    if href and not href.startswith("javascript"):
                        source_url = href
                
                electricians.append(Electrician(
                    name=self._clean_text(name),
                    phone=phone,
                    city=city,
                    state=state,
                    address=address,
                    rating=rating,
                    source="justdial_selenium",
                    source_url=source_url,
                ))
                
            except Exception as e:
                self.logger.debug(f"Error parsing listing: {e}")
                continue
        
        return electricians
    
    def scrape_city(self, city: str, state: str, max_pages: int = 3) -> ScrapeResult:
        """Scrape JustDial using Selenium."""
        all_electricians = []
        pages_scraped = 0
        
        try:
            categories = ["Electricians", "Electrical-Contractors"]
            
            for category in categories:
                url = self._build_url(city, category)
                self.logger.info(f"Scraping {url}")
                
                self._get_page(url, wait_for=".cntanr, .store-details")
                pages_scraped += 1
                
                electricians = self._extract_listings(city, state)
                all_electricians.extend(electricians)
                
                # Try to paginate
                for page in range(2, max_pages + 1):
                    try:
                        # Look for next page button
                        next_btn = self._find_element(f"a[href*='page-{page}'], .pagenum a")
                        if next_btn:
                            next_btn.click()
                            time.sleep(2)
                            pages_scraped += 1
                            electricians = self._extract_listings(city, state)
                            if not electricians:
                                break
                            all_electricians.extend(electricians)
                        else:
                            break
                    except Exception:
                        break
            
            # Remove duplicates
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
            self.logger.error(f"Error scraping {city}, {state}: {e}")
            return ScrapeResult(
                success=False,
                source=self.name,
                city=city,
                state=state,
                error_message=str(e),
                pages_scraped=pages_scraped,
            )
