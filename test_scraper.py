#!/usr/bin/env python3
"""
Quick test script to verify the scraper setup.
Run this to test individual scrapers on a single city.
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.scrapers.justdial_scraper import JustDialScraper
from src.scrapers.indiamart_scraper import IndiaMARTScraper
from src.scrapers.sulekha_scraper import SulekhaScraper
from src.storage import DataStorage


def test_scraper(scraper_class, city: str = "Mumbai", state: str = "Maharashtra"):
    """Test a single scraper."""
    print(f"\n{'='*50}")
    print(f"Testing {scraper_class.__name__}")
    print(f"City: {city}, State: {state}")
    print(f"{'='*50}")
    
    scraper = scraper_class()
    
    try:
        result = scraper.scrape_city(city, state)
        
        print(f"\nResult:")
        print(f"  Success: {result.success}")
        print(f"  Pages Scraped: {result.pages_scraped}")
        print(f"  Electricians Found: {len(result.electricians)}")
        
        if result.error_message:
            print(f"  Error: {result.error_message}")
        
        if result.electricians:
            print(f"\nSample Records (first 3):")
            for e in result.electricians[:3]:
                print(f"  - {e.name}: {e.phone}")
                if e.address:
                    print(f"    Address: {e.address[:50]}...")
                if e.rating:
                    print(f"    Rating: {e.rating}")
        
        return result.electricians
        
    except Exception as e:
        print(f"Error: {e}")
        return []
    
    finally:
        scraper.close()


def main():
    """Run tests on all scrapers."""
    city = "Delhi"
    state = "Delhi"
    
    if len(sys.argv) > 1:
        city = sys.argv[1]
    if len(sys.argv) > 2:
        state = sys.argv[2]
    
    all_electricians = []
    
    # Test each scraper
    scrapers_to_test = [
        JustDialScraper,
        IndiaMARTScraper,
        SulekhaScraper,
    ]
    
    for scraper_class in scrapers_to_test:
        try:
            electricians = test_scraper(scraper_class, city, state)
            all_electricians.extend(electricians)
        except Exception as e:
            print(f"Failed to test {scraper_class.__name__}: {e}")
    
    # Save results
    if all_electricians:
        print(f"\n{'='*50}")
        print(f"TOTAL RESULTS: {len(all_electricians)} electricians")
        print(f"{'='*50}")
        
        storage = DataStorage()
        
        # Save to CSV
        csv_path = storage.save_to_csv(all_electricians, filename=f"test_{city.lower()}.csv", append=False)
        print(f"\nSaved to: {csv_path}")
        
        # Save to database
        saved = storage.save_to_database(all_electricians)
        print(f"Saved {saved} new records to database")
        
        # Show unique count
        unique = list({e.get_unique_key(): e for e in all_electricians}.values())
        print(f"Unique electricians (by phone): {len(unique)}")
    else:
        print("\nNo electricians found. Try a different city or check your internet connection.")


if __name__ == "__main__":
    main()
