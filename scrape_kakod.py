#!/usr/bin/env python3
"""
Scraper for fetching electricians in Kakod area, Uttar Pradesh.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
from bs4 import BeautifulSoup
import re
import time
import random
import json
import csv
from datetime import datetime
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


def scrape_justdial(city, state, area=None):
    """Scrape JustDial for electricians."""
    electricians = []
    city_slug = city.lower().replace(" ", "-")
    area_slug = area.lower().replace(" ", "-") if area else ""
    
    urls = [
        f"https://www.justdial.com/{city_slug}/Electricians-in-{area_slug}" if area else f"https://www.justdial.com/{city_slug}/electricians",
        f"https://www.justdial.com/{city_slug}/Electrical-Contractors-in-{area_slug}" if area else f"https://www.justdial.com/{city_slug}/electrical-contractors",
        f"https://www.justdial.com/{area_slug}/electricians" if area else None,
    ]
    
    # Filter out None URLs
    urls = [u for u in urls if u]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    for url in urls:
        print(f"  Scraping: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, "lxml")
            
            # Find listings
            for div in soup.find_all(["div", "li"], class_=re.compile(r"cntanr|store|result|jsx|resultbox")):
                text = div.get_text()
                phones = extract_phone_numbers(text)
                
                if phones:
                    name_elem = div.find(["h2", "h3", "span", "a"], class_=re.compile(r"name|title|lng_cont|store-name"))
                    name = name_elem.get_text(strip=True) if name_elem else "Electrician"
                    
                    # Get address
                    addr_elem = div.find(["span", "p"], class_=re.compile(r"addr|location|mrehgnbx"))
                    address = addr_elem.get_text(strip=True)[:200] if addr_elem else f"{area}, {city}" if area else city
                    
                    # Get rating if available
                    rating_elem = div.find(["span"], class_=re.compile(r"rating|star|rate"))
                    rating = None
                    if rating_elem:
                        try:
                            rating = float(re.search(r'[\d.]+', rating_elem.get_text()).group())
                        except:
                            pass
                    
                    for phone in phones[:1]:  # Take first phone only
                        electricians.append(Electrician(
                            name=name[:100],
                            phone=phone,
                            city=city,
                            state=state,
                            address=address,
                            rating=rating,
                            source="justdial",
                        ))
            
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            print(f"    Error: {e}")
    
    return electricians


def scrape_indiamart(city, state, area=None):
    """Scrape IndiaMART for electricians."""
    electricians = []
    city_slug = city.lower().replace(" ", "-")
    area_slug = area.lower().replace(" ", "-") if area else ""
    
    urls = [
        f"https://dir.indiamart.com/{city_slug}/electricians.html",
        f"https://dir.indiamart.com/{city_slug}/electrical-contractors.html",
    ]
    
    if area:
        urls.extend([
            f"https://dir.indiamart.com/{area_slug}/electricians.html",
            f"https://dir.indiamart.com/{area_slug}/electrical-contractors.html",
        ])
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    for url in urls:
        print(f"  Scraping: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, "lxml")
            
            for div in soup.find_all(["div", "li"], class_=re.compile(r"lst|card|company|pname")):
                text = div.get_text()
                phones = extract_phone_numbers(text)
                
                if phones:
                    name_elem = div.find(["a", "h2", "h3"], class_=re.compile(r"name|pnm|company|lcname"))
                    name = name_elem.get_text(strip=True) if name_elem else "Electrical Service"
                    
                    addr_elem = div.find(["span", "p", "div"], class_=re.compile(r"addr|location|city|cloc"))
                    address = addr_elem.get_text(strip=True)[:200] if addr_elem else f"{area}, {city}" if area else city
                    
                    for phone in phones[:1]:
                        electricians.append(Electrician(
                            name=name[:100],
                            phone=phone,
                            city=city,
                            state=state,
                            address=address,
                            source="indiamart",
                        ))
            
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            print(f"    Error: {e}")
    
    return electricians


def scrape_sulekha(city, state, area=None):
    """Scrape Sulekha for electricians."""
    electricians = []
    city_slug = city.lower().replace(" ", "-")
    area_slug = area.lower().replace(" ", "-") if area else ""
    
    urls = [
        f"https://www.sulekha.com/electricians/{city_slug}",
    ]
    
    if area:
        urls.append(f"https://www.sulekha.com/electricians/{area_slug}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    }
    
    for url in urls:
        print(f"  Scraping: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, "lxml")
            
            for div in soup.find_all(["div", "article"], class_=re.compile(r"vendor|card|listing|provider|vendorbox")):
                text = div.get_text()
                phones = extract_phone_numbers(text)
                
                if phones:
                    name_elem = div.find(["h2", "h3", "a"], class_=re.compile(r"name|title|vendor"))
                    name = name_elem.get_text(strip=True) if name_elem else "Electrician"
                    
                    addr_elem = div.find(["span", "p"], class_=re.compile(r"addr|location|area"))
                    address = addr_elem.get_text(strip=True)[:200] if addr_elem else f"{area}, {city}" if area else city
                    
                    for phone in phones[:1]:
                        electricians.append(Electrician(
                            name=name[:100],
                            phone=phone,
                            city=city,
                            state=state,
                            address=address,
                            source="sulekha",
                        ))
            
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            print(f"    Error: {e}")
    
    return electricians


def scrape_google_search(city, state, area=None):
    """Scrape Google search results for electricians."""
    electricians = []
    
    search_queries = [
        f"electrician {area} {city} {state} contact number" if area else f"electrician {city} {state} contact number",
        f"bijli mistri {area} {city}" if area else f"bijli mistri {city}",
        f"electrical contractor {area} {city} phone" if area else f"electrical contractor {city} phone",
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    for query in search_queries:
        url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
        print(f"  Searching: {query}")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, "lxml")
            
            text = soup.get_text()
            phones = extract_phone_numbers(text)
            
            for phone in phones[:5]:  # Limit to 5 numbers per search
                electricians.append(Electrician(
                    name=f"Electrician in {area or city}",
                    phone=phone,
                    city=city,
                    state=state,
                    address=f"{area}, {city}" if area else city,
                    source="google_search",
                ))
            
            time.sleep(random.uniform(2, 4))  # Longer delay for Google
            
        except Exception as e:
            print(f"    Error: {e}")
    
    return electricians


def export_results(electricians, area, city):
    """Export results to CSV and JSON files."""
    if not electricians:
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    area_slug = area.lower().replace(" ", "_") if area else city.lower().replace(" ", "_")
    
    # Export to CSV
    csv_file = Path(__file__).parent / "output" / f"{area_slug}_electricians_{timestamp}.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Phone", "City", "State", "Address", "Rating", "Source"])
        for e in electricians:
            writer.writerow([e.name, e.phone, e.city, e.state, e.address, e.rating, e.source])
    print(f"\n📄 CSV exported: {csv_file}")
    
    # Export to JSON
    json_file = Path(__file__).parent / "output" / f"{area_slug}_electricians_{timestamp}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "name": e.name,
            "phone": e.phone,
            "city": e.city,
            "state": e.state,
            "address": e.address,
            "rating": e.rating,
            "source": e.source,
        } for e in electricians], f, indent=2, ensure_ascii=False)
    print(f"📄 JSON exported: {json_file}")


def main():
    """Main function to scrape Kakod area electricians."""
    print("="*60)
    print("🔌 Electricians Scraper - Kakod, Uttar Pradesh")
    print("="*60)
    
    # Target location
    area = "Kakod"
    city = "Greater Noida"  # Kakod is near Greater Noida
    state = "Uttar Pradesh"
    
    # Also search nearby areas
    nearby_areas = ["Kakod", "Jewar", "Greater Noida", "Dadri"]
    
    all_electricians = []
    
    print(f"\n📍 Searching for electricians in {area} and nearby areas...")
    print("-"*60)
    
    for search_area in nearby_areas:
        print(f"\n🔍 Searching: {search_area}, {state}")
        area_electricians = []
        
        # Try each source
        try:
            area_electricians.extend(scrape_justdial(city, state, search_area))
        except Exception as e:
            print(f"  JustDial error: {e}")
        
        try:
            area_electricians.extend(scrape_indiamart(city, state, search_area))
        except Exception as e:
            print(f"  IndiaMART error: {e}")
        
        try:
            area_electricians.extend(scrape_sulekha(city, state, search_area))
        except Exception as e:
            print(f"  Sulekha error: {e}")
        
        try:
            area_electricians.extend(scrape_google_search(city, state, search_area))
        except Exception as e:
            print(f"  Google search error: {e}")
        
        if area_electricians:
            print(f"  ✅ Found {len(area_electricians)} results")
            all_electricians.extend(area_electricians)
        else:
            print(f"  ⚠️ No data found for {search_area}")
        
        time.sleep(1)
    
    # Deduplicate by phone number
    unique_electricians = list({e.phone: e for e in all_electricians}.values())
    
    print("\n" + "="*60)
    print("📊 SCRAPING RESULTS")
    print("="*60)
    
    if unique_electricians:
        print(f"\n✅ Found {len(unique_electricians)} unique electricians")
        
        # Save to database
        saved = storage.save_to_database(unique_electricians)
        print(f"💾 Saved {saved} new records to database")
        
        # Export to files
        export_results(unique_electricians, area, city)
        
        # Display results
        print("\n📋 Electricians Found:")
        print("-"*60)
        for i, e in enumerate(unique_electricians[:20], 1):  # Show first 20
            print(f"{i}. {e.name}")
            print(f"   📞 {e.phone}")
            print(f"   📍 {e.address or e.city}")
            print(f"   🌐 Source: {e.source}")
            print()
        
        if len(unique_electricians) > 20:
            print(f"... and {len(unique_electricians) - 20} more (see exported files)")
    else:
        print("\n⚠️ No electricians found. This could be because:")
        print("   - Websites are blocking automated requests")
        print("   - Limited data available for this area")
        print("   - Network connectivity issues")
        print("\nTry using proxies or VPN for better results.")
    
    print("\n🎉 Scraping complete!")


if __name__ == "__main__":
    main()
