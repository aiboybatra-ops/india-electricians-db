#!/usr/bin/env python3
"""
Verified Surat Electricians Scraper
Extracts real data with source URLs for verification
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
from dataclasses import dataclass, asdict
from typing import List, Optional

# Output files
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

@dataclass
class VerifiedElectrician:
    name: str
    phone: str
    address: str
    city: str = "Surat"
    state: str = "Gujarat"
    service_type: str = ""
    rating: Optional[float] = None
    reviews: Optional[int] = None
    source: str = ""
    source_url: str = ""
    verified: bool = False
    scraped_at: str = ""

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
    }

def extract_indian_phone(text: str) -> List[str]:
    """Extract valid Indian phone numbers."""
    patterns = [
        r'\+91[\s\-]?[6-9]\d{9}',
        r'91[\s\-]?[6-9]\d{9}',
        r'0[6-9]\d{9}',
        r'[6-9]\d{9}',
        r'[6-9]\d{4}[\s\-]?\d{5}',
    ]
    phones = set()
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for m in matches:
            digits = re.sub(r'\D', '', m)
            if len(digits) >= 10:
                phone = digits[-10:]
                # Validate: Indian mobile starts with 6-9
                if phone[0] in '6789':
                    phones.add(phone)
    return list(phones)

def is_valid_phone(phone: str) -> bool:
    """Validate Indian phone number."""
    if not phone or len(phone) != 10:
        return False
    if phone[0] not in '6789':
        return False
    # Check it's not a fake pattern like 9999999999
    if len(set(phone)) < 4:
        return False
    return True

def is_valid_name(name: str) -> bool:
    """Check if name looks legitimate."""
    if not name or len(name) < 3:
        return False
    # Filter out generic/placeholder names
    invalid_patterns = ['test', 'sample', 'demo', 'example', 'null', 'undefined']
    name_lower = name.lower()
    for pattern in invalid_patterns:
        if pattern in name_lower:
            return False
    return True


def scrape_justdial_surat(category: str, service_type: str) -> List[VerifiedElectrician]:
    """Scrape JustDial for Surat electricians."""
    results = []
    base_url = f"https://www.justdial.com/Surat/{category}"
    
    print(f"\n📍 JustDial: {category}")
    print(f"   URL: {base_url}")
    
    try:
        time.sleep(random.uniform(2, 4))
        response = requests.get(base_url, headers=get_headers(), timeout=30)
        
        if response.status_code != 200:
            print(f"   ❌ Status: {response.status_code}")
            return results
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        # JustDial uses various class patterns
        listings = soup.find_all(['li', 'div'], class_=re.compile(r'cntanr|resultbox|jsx-'))
        
        for listing in listings:
            try:
                text = listing.get_text(separator=' ', strip=True)
                phones = extract_indian_phone(text)
                
                if not phones:
                    continue
                
                # Extract name
                name_elem = listing.find(['a', 'span', 'h2'], class_=re.compile(r'lng_cont_name|store-name|title'))
                if not name_elem:
                    name_elem = listing.find(['h2', 'h3', 'a'])
                
                name = name_elem.get_text(strip=True)[:100] if name_elem else None
                
                if not is_valid_name(name):
                    continue
                
                # Extract address
                addr_elem = listing.find(['span', 'p'], class_=re.compile(r'cont_fl_addr|address|area'))
                address = addr_elem.get_text(strip=True)[:200] if addr_elem else "Surat, Gujarat"
                
                # Extract rating
                rating = None
                rating_elem = listing.find(['span'], class_=re.compile(r'green-box|rating|star'))
                if rating_elem:
                    rating_text = rating_elem.get_text(strip=True)
                    rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                    if rating_match:
                        rating = float(rating_match.group(1))
                
                # Extract detail URL
                link_elem = listing.find('a', href=True)
                detail_url = ""
                if link_elem:
                    href = link_elem.get('href', '')
                    if href.startswith('/'):
                        detail_url = f"https://www.justdial.com{href}"
                    elif href.startswith('http'):
                        detail_url = href
                
                for phone in phones[:1]:  # Take first valid phone
                    if is_valid_phone(phone):
                        results.append(VerifiedElectrician(
                            name=name,
                            phone=phone,
                            address=address,
                            service_type=service_type,
                            rating=rating,
                            source="JustDial",
                            source_url=detail_url or base_url,
                            verified=True,
                            scraped_at=datetime.now().isoformat()
                        ))
                        
            except Exception as e:
                continue
        
        print(f"   ✅ Found {len(results)} verified listings")
        
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Request error: {e}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    return results


def scrape_sulekha_surat(category: str, service_type: str) -> List[VerifiedElectrician]:
    """Scrape Sulekha for Surat electricians."""
    results = []
    base_url = f"https://www.sulekha.com/{category}/surat"
    
    print(f"\n📍 Sulekha: {category}")
    print(f"   URL: {base_url}")
    
    try:
        time.sleep(random.uniform(2, 4))
        response = requests.get(base_url, headers=get_headers(), timeout=30)
        
        if response.status_code != 200:
            print(f"   ❌ Status: {response.status_code}")
            return results
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        listings = soup.find_all(['div', 'article'], class_=re.compile(r'vendor|card|listing|merchant'))
        
        for listing in listings:
            try:
                text = listing.get_text(separator=' ', strip=True)
                phones = extract_indian_phone(text)
                
                if not phones:
                    continue
                
                name_elem = listing.find(['h2', 'h3', 'a', 'span'], class_=re.compile(r'name|title|merchant'))
                name = name_elem.get_text(strip=True)[:100] if name_elem else None
                
                if not is_valid_name(name):
                    continue
                
                addr_elem = listing.find(['span', 'p', 'div'], class_=re.compile(r'address|location|area'))
                address = addr_elem.get_text(strip=True)[:200] if addr_elem else "Surat, Gujarat"
                
                link_elem = listing.find('a', href=True)
                detail_url = ""
                if link_elem:
                    href = link_elem.get('href', '')
                    if href.startswith('/'):
                        detail_url = f"https://www.sulekha.com{href}"
                    elif href.startswith('http'):
                        detail_url = href
                
                for phone in phones[:1]:
                    if is_valid_phone(phone):
                        results.append(VerifiedElectrician(
                            name=name,
                            phone=phone,
                            address=address,
                            service_type=service_type,
                            source="Sulekha",
                            source_url=detail_url or base_url,
                            verified=True,
                            scraped_at=datetime.now().isoformat()
                        ))
                        
            except Exception as e:
                continue
        
        print(f"   ✅ Found {len(results)} verified listings")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    return results


def scrape_indiamart_surat(category: str, service_type: str) -> List[VerifiedElectrician]:
    """Scrape IndiaMART for Surat electricians."""
    results = []
    base_url = f"https://dir.indiamart.com/surat/{category}.html"
    
    print(f"\n📍 IndiaMART: {category}")
    print(f"   URL: {base_url}")
    
    try:
        time.sleep(random.uniform(2, 4))
        response = requests.get(base_url, headers=get_headers(), timeout=30)
        
        if response.status_code != 200:
            print(f"   ❌ Status: {response.status_code}")
            return results
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        listings = soup.find_all(['div', 'li'], class_=re.compile(r'lst|card|company|lcname|prd-'))
        
        for listing in listings:
            try:
                text = listing.get_text(separator=' ', strip=True)
                phones = extract_indian_phone(text)
                
                if not phones:
                    continue
                
                name_elem = listing.find(['a', 'h2', 'h3', 'span'], class_=re.compile(r'lcname|company|pnm|title'))
                name = name_elem.get_text(strip=True)[:100] if name_elem else None
                
                if not is_valid_name(name):
                    continue
                
                addr_elem = listing.find(['span', 'p'], class_=re.compile(r'lcity|address|location'))
                address = addr_elem.get_text(strip=True)[:200] if addr_elem else "Surat, Gujarat"
                
                link_elem = listing.find('a', href=True)
                detail_url = ""
                if link_elem:
                    href = link_elem.get('href', '')
                    if href.startswith('http'):
                        detail_url = href
                
                for phone in phones[:1]:
                    if is_valid_phone(phone):
                        results.append(VerifiedElectrician(
                            name=name,
                            phone=phone,
                            address=address,
                            service_type=service_type,
                            source="IndiaMART",
                            source_url=detail_url or base_url,
                            verified=True,
                            scraped_at=datetime.now().isoformat()
                        ))
                        
            except Exception as e:
                continue
        
        print(f"   ✅ Found {len(results)} verified listings")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    return results


def deduplicate_by_phone(electricians: List[VerifiedElectrician]) -> List[VerifiedElectrician]:
    """Remove duplicates based on phone number."""
    seen = {}
    for e in electricians:
        if e.phone not in seen:
            seen[e.phone] = e
    return list(seen.values())


def save_to_csv(electricians: List[VerifiedElectrician], filename: str):
    """Save to CSV with all details for verification."""
    filepath = OUTPUT_DIR / filename
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Name', 'Phone', 'Address', 'City', 'State', 
            'Service Type', 'Rating', 'Source', 'Source URL', 'Scraped At'
        ])
        
        for e in electricians:
            writer.writerow([
                e.name, e.phone, e.address, e.city, e.state,
                e.service_type, e.rating or '', e.source, e.source_url, e.scraped_at
            ])
    
    print(f"\n📁 Saved to: {filepath}")
    return filepath


def save_to_json(electricians: List[VerifiedElectrician], filename: str):
    """Save to JSON for detailed review."""
    filepath = OUTPUT_DIR / filename
    
    data = {
        "scraped_at": datetime.now().isoformat(),
        "location": "Surat, Gujarat",
        "total_records": len(electricians),
        "records": [asdict(e) for e in electricians]
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"📁 Saved to: {filepath}")
    return filepath


def print_verification_report(electricians: List[VerifiedElectrician]):
    """Print report with source URLs for verification."""
    print("\n" + "=" * 80)
    print("📋 VERIFICATION REPORT - Surat Electricians")
    print("=" * 80)
    
    # Group by source
    by_source = {}
    for e in electricians:
        if e.source not in by_source:
            by_source[e.source] = []
        by_source[e.source].append(e)
    
    for source, records in by_source.items():
        print(f"\n🌐 {source} ({len(records)} records)")
        print("-" * 60)
        
        for i, e in enumerate(records[:10], 1):  # Show first 10 from each source
            print(f"\n{i}. {e.name}")
            print(f"   📞 Phone: {e.phone}")
            print(f"   📍 Address: {e.address}")
            print(f"   🔧 Service: {e.service_type}")
            if e.rating:
                print(f"   ⭐ Rating: {e.rating}")
            print(f"   🔗 Verify: {e.source_url}")
        
        if len(records) > 10:
            print(f"\n   ... and {len(records) - 10} more from {source}")
    
    print("\n" + "=" * 80)


def main():
    print("\n" + "=" * 80)
    print("🔌 VERIFIED SURAT ELECTRICIANS SCRAPER")
    print("=" * 80)
    print("📍 Location: Surat, Gujarat, India")
    print("🎯 Extracting real data with verifiable source links")
    print("=" * 80)
    
    all_electricians = []
    
    # Categories to scrape
    print("\n🌐 Scraping from multiple sources...")
    
    # JustDial categories
    justdial_cats = [
        ("Electricians", "Electrician"),
        ("Electrical-Contractors", "Electrical Contractor"),
        ("Electric-Meter-Dealers", "Meter Installer"),
        ("House-Wiring-Contractors", "House Wiring"),
    ]
    
    for cat, svc in justdial_cats:
        results = scrape_justdial_surat(cat, svc)
        all_electricians.extend(results)
        time.sleep(random.uniform(1, 2))
    
    # Sulekha categories
    sulekha_cats = [
        ("electricians", "Electrician"),
        ("electrical-contractors", "Electrical Contractor"),
    ]
    
    for cat, svc in sulekha_cats:
        results = scrape_sulekha_surat(cat, svc)
        all_electricians.extend(results)
        time.sleep(random.uniform(1, 2))
    
    # IndiaMART categories
    indiamart_cats = [
        ("electricians", "Electrician"),
        ("electrical-contractors", "Electrical Contractor"),
    ]
    
    for cat, svc in indiamart_cats:
        results = scrape_indiamart_surat(cat, svc)
        all_electricians.extend(results)
        time.sleep(random.uniform(1, 2))
    
    # Deduplicate
    unique = deduplicate_by_phone(all_electricians)
    
    print(f"\n📊 SCRAPING SUMMARY")
    print("-" * 40)
    print(f"Total scraped: {len(all_electricians)}")
    print(f"After deduplication: {len(unique)}")
    
    if unique:
        # Save to files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = save_to_csv(unique, f"surat_electricians_verified_{timestamp}.csv")
        json_file = save_to_json(unique, f"surat_electricians_verified_{timestamp}.json")
        
        # Print verification report
        print_verification_report(unique)
        
        print(f"\n✅ FILES FOR VERIFICATION:")
        print(f"   CSV:  {csv_file}")
        print(f"   JSON: {json_file}")
        print("\n👉 Open the CSV/JSON files to verify each record with its source URL")
    else:
        print("\n⚠️  No records found from web scraping.")
        print("   This usually happens due to:")
        print("   1. Anti-bot protection (CAPTCHAs, rate limiting)")
        print("   2. IP blocking")
        print("   3. Website structure changes")
        print("\n💡 ALTERNATIVES:")
        print("   1. Use proxy support (see PROXY_SETUP.md)")
        print("   2. Use the Google Places API with your API key")
        print("   3. Manual data collection from verified sources")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
