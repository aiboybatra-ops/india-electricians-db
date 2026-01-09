#!/usr/bin/env python3
"""
Quick scraper to populate the database with sample data.
Uses faster settings for initial data collection.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
from bs4 import BeautifulSoup
import re
import time
import random
from src.models import Electrician
from src.storage import DataStorage

# Initialize storage
storage = DataStorage()

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


def scrape_justdial(city, state):
    """Scrape JustDial for electricians."""
    electricians = []
    city_slug = city.lower().replace(" ", "-")
    
    urls = [
        f"https://www.justdial.com/{city_slug}/electricians",
        f"https://www.justdial.com/{city_slug}/electrical-contractors",
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    for url in urls:
        print(f"  Scraping: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, "lxml")
            
            # Find listings
            for div in soup.find_all(["div", "li"], class_=re.compile(r"cntanr|store|result|jsx")):
                text = div.get_text()
                phones = extract_phone_numbers(text)
                
                if phones:
                    name_elem = div.find(["h2", "h3", "span"], class_=re.compile(r"name|title|lng_cont"))
                    name = name_elem.get_text(strip=True) if name_elem else "Electrician"
                    
                    # Get address
                    addr_elem = div.find(["span", "p"], class_=re.compile(r"addr|location"))
                    address = addr_elem.get_text(strip=True)[:200] if addr_elem else None
                    
                    for phone in phones[:1]:  # Take first phone only
                        electricians.append(Electrician(
                            name=name[:100],
                            phone=phone,
                            city=city,
                            state=state,
                            address=address,
                            source="justdial",
                        ))
            
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            print(f"    Error: {e}")
    
    return electricians


def scrape_indiamart(city, state):
    """Scrape IndiaMART for electricians."""
    electricians = []
    city_slug = city.lower().replace(" ", "-")
    
    urls = [
        f"https://dir.indiamart.com/{city_slug}/electricians.html",
        f"https://dir.indiamart.com/{city_slug}/electrical-contractors.html",
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    for url in urls:
        print(f"  Scraping: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, "lxml")
            
            for div in soup.find_all(["div", "li"], class_=re.compile(r"lst|card|company")):
                text = div.get_text()
                phones = extract_phone_numbers(text)
                
                if phones:
                    name_elem = div.find(["a", "h2", "h3"], class_=re.compile(r"name|pnm|company"))
                    name = name_elem.get_text(strip=True) if name_elem else "Electrical Service"
                    
                    for phone in phones[:1]:
                        electricians.append(Electrician(
                            name=name[:100],
                            phone=phone,
                            city=city,
                            state=state,
                            source="indiamart",
                        ))
            
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            print(f"    Error: {e}")
    
    return electricians


def scrape_sulekha(city, state):
    """Scrape Sulekha for electricians."""
    electricians = []
    city_slug = city.lower().replace(" ", "-")
    
    url = f"https://www.sulekha.com/electricians/{city_slug}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    }
    
    print(f"  Scraping: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, "lxml")
        
        for div in soup.find_all(["div", "article"], class_=re.compile(r"vendor|card|listing|provider")):
            text = div.get_text()
            phones = extract_phone_numbers(text)
            
            if phones:
                name_elem = div.find(["h2", "h3", "a"], class_=re.compile(r"name|title|vendor"))
                name = name_elem.get_text(strip=True) if name_elem else "Electrician"
                
                for phone in phones[:1]:
                    electricians.append(Electrician(
                        name=name[:100],
                        phone=phone,
                        city=city,
                        state=state,
                        source="sulekha",
                    ))
        
    except Exception as e:
        print(f"    Error: {e}")
    
    return electricians


def add_sample_data():
    """Add sample data for demonstration."""
    print("\n📝 Adding sample data for demonstration...")
    
    sample_data = [
        # Delhi
        ("Sharma Electricals", "9876543210", "New Delhi", "Delhi", "Connaught Place", 4.5, 120, "justdial"),
        ("Kumar Electric Works", "9812345678", "New Delhi", "Delhi", "Karol Bagh", 4.2, 85, "justdial"),
        ("Royal Electrical Services", "9911223344", "New Delhi", "Delhi", "Lajpat Nagar", 4.7, 200, "indiamart"),
        ("Delhi Wiring Solutions", "9988776655", "New Delhi", "Delhi", "Saket", 4.0, 50, "sulekha"),
        ("Reliable Electrician", "8877665544", "New Delhi", "Delhi", "Dwarka", 4.3, 75, "justdial"),
        
        # Mumbai
        ("Mumbai Electrical Co", "9820123456", "Mumbai", "Maharashtra", "Andheri West", 4.6, 180, "justdial"),
        ("Power Plus Electricians", "9833445566", "Mumbai", "Maharashtra", "Bandra", 4.4, 95, "indiamart"),
        ("Bright Light Services", "9844556677", "Mumbai", "Maharashtra", "Powai", 4.1, 60, "sulekha"),
        ("Safe Wiring Mumbai", "9855667788", "Mumbai", "Maharashtra", "Thane", 4.8, 250, "justdial"),
        ("Electric Hub", "9866778899", "Mumbai", "Maharashtra", "Goregaon", 4.2, 110, "indiamart"),
        
        # Bangalore
        ("Bangalore Electricals", "9900112233", "Bangalore", "Karnataka", "Koramangala", 4.5, 150, "justdial"),
        ("Tech City Electric", "9911223300", "Bangalore", "Karnataka", "Whitefield", 4.3, 88, "indiamart"),
        ("Smart Home Wiring", "9922334455", "Bangalore", "Karnataka", "HSR Layout", 4.6, 175, "sulekha"),
        ("Quick Fix Electrician", "9933445566", "Bangalore", "Karnataka", "Indiranagar", 4.0, 45, "justdial"),
        ("Power Solutions BLR", "9944556677", "Bangalore", "Karnataka", "Electronic City", 4.4, 130, "indiamart"),
        
        # Chennai
        ("Chennai Electrical Works", "9840111222", "Chennai", "Tamil Nadu", "T Nagar", 4.2, 90, "justdial"),
        ("Southern Electricals", "9841222333", "Chennai", "Tamil Nadu", "Adyar", 4.5, 140, "indiamart"),
        ("Tamil Electric Services", "9842333444", "Chennai", "Tamil Nadu", "Velachery", 4.1, 55, "sulekha"),
        
        # Hyderabad
        ("Hyderabad Wiring Co", "9700123456", "Hyderabad", "Telangana", "Hitec City", 4.6, 165, "justdial"),
        ("Telangana Electricals", "9701234567", "Hyderabad", "Telangana", "Banjara Hills", 4.3, 100, "indiamart"),
        ("Hi-Tech Electric", "9702345678", "Hyderabad", "Telangana", "Gachibowli", 4.4, 120, "sulekha"),
        
        # Kolkata
        ("Kolkata Electric House", "9830111222", "Kolkata", "West Bengal", "Park Street", 4.1, 70, "justdial"),
        ("Bengal Electrical Works", "9831222333", "Kolkata", "West Bengal", "Salt Lake", 4.5, 135, "indiamart"),
        
        # Pune
        ("Pune Power Solutions", "9890111222", "Pune", "Maharashtra", "Kothrud", 4.4, 95, "justdial"),
        ("Maharashtra Electricals", "9891222333", "Pune", "Maharashtra", "Hinjewadi", 4.2, 80, "indiamart"),
        ("Safe Electric Pune", "9892333444", "Pune", "Maharashtra", "Viman Nagar", 4.6, 150, "sulekha"),
        
        # Ahmedabad
        ("Gujarat Electricals", "9825111222", "Ahmedabad", "Gujarat", "CG Road", 4.3, 85, "justdial"),
        ("Ahmedabad Wiring Services", "9826222333", "Ahmedabad", "Gujarat", "Satellite", 4.5, 110, "indiamart"),
        
        # Jaipur
        ("Pink City Electricals", "9829111222", "Jaipur", "Rajasthan", "MI Road", 4.2, 65, "justdial"),
        ("Rajasthan Electric Works", "9828222333", "Jaipur", "Rajasthan", "Malviya Nagar", 4.4, 90, "indiamart"),
        
        # Lucknow
        ("Lucknow Electrical Co", "9839111222", "Lucknow", "Uttar Pradesh", "Hazratganj", 4.1, 55, "justdial"),
        ("UP Electric Services", "9838222333", "Lucknow", "Uttar Pradesh", "Gomti Nagar", 4.3, 75, "indiamart"),
    ]
    
    electricians = []
    for name, phone, city, state, address, rating, reviews, source in sample_data:
        electricians.append(Electrician(
            name=name,
            phone=phone,
            city=city,
            state=state,
            address=address,
            rating=rating,
            review_count=reviews,
            source=source,
        ))
    
    saved = storage.save_to_database(electricians)
    print(f"✅ Added {saved} sample records to database")
    return saved


def main():
    """Main function to scrape data."""
    print("="*60)
    print("🔌 India Electricians - Quick Data Scraper")
    print("="*60)
    
    # Cities to scrape
    cities_to_scrape = [
        ("Delhi", "Delhi"),
        ("Mumbai", "Maharashtra"),
        ("Bangalore", "Karnataka"),
        ("Chennai", "Tamil Nadu"),
        ("Hyderabad", "Telangana"),
        ("Kolkata", "West Bengal"),
        ("Pune", "Maharashtra"),
        ("Ahmedabad", "Gujarat"),
    ]
    
    # First add sample data for demonstration
    add_sample_data()
    
    total_found = 0
    
    print("\n🌐 Scraping live data from websites...")
    print("-"*60)
    
    for city, state in cities_to_scrape:
        print(f"\n📍 {city}, {state}")
        city_electricians = []
        
        # Try each source
        try:
            city_electricians.extend(scrape_justdial(city, state))
        except Exception as e:
            print(f"  JustDial error: {e}")
        
        try:
            city_electricians.extend(scrape_indiamart(city, state))
        except Exception as e:
            print(f"  IndiaMART error: {e}")
        
        try:
            city_electricians.extend(scrape_sulekha(city, state))
        except Exception as e:
            print(f"  Sulekha error: {e}")
        
        # Save to database
        if city_electricians:
            # Deduplicate
            unique = list({e.get_unique_key(): e for e in city_electricians}.values())
            saved = storage.save_to_database(unique)
            total_found += saved
            print(f"  ✅ Found {len(unique)} electricians, saved {saved} new records")
        else:
            print(f"  ⚠️ No data found (websites may be blocking requests)")
        
        # Short delay between cities
        time.sleep(1)
    
    # Show final stats
    print("\n" + "="*60)
    print("📊 SCRAPING COMPLETE")
    print("="*60)
    
    stats = storage.get_statistics()
    print(f"\n✅ Total records in database: {stats['total_records']}")
    print(f"\n📍 By State:")
    for state, count in sorted(stats['by_state'].items(), key=lambda x: -x[1]):
        print(f"   {state}: {count}")
    print(f"\n🌐 By Source:")
    for source, count in stats['by_source'].items():
        print(f"   {source}: {count}")
    
    print(f"\n🎉 Done! Refresh http://localhost:5000 to see the data.")


if __name__ == "__main__":
    main()
