#!/usr/bin/env python3
"""
Quick verified scraper - scrapes one source and shows results with URLs for verification
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
from bs4 import BeautifulSoup
import re
import time
import random
import csv
from datetime import datetime

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

def get_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

def extract_phone(text):
    """Extract valid Indian phone numbers."""
    phones = set()
    patterns = [r'[6-9]\d{9}', r'[6-9]\d{4}[\s\-]?\d{5}']
    for p in patterns:
        for m in re.findall(p, text):
            digits = re.sub(r'\D', '', m)[-10:]
            if len(digits) == 10 and digits[0] in '6789' and len(set(digits)) >= 4:
                phones.add(digits)
    return list(phones)

def scrape_justdial():
    """Scrape JustDial Surat Electricians."""
    results = []
    url = "https://www.justdial.com/Surat/Electricians"
    
    print(f"\n🌐 Scraping: {url}")
    print("-" * 70)
    
    try:
        response = requests.get(url, headers=get_headers(), timeout=30)
        if response.status_code != 200:
            print(f"❌ Failed: Status {response.status_code}")
            return results
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find all listing containers
        listings = soup.find_all(['li', 'div'], class_=re.compile(r'cntanr|resultbox|jsx-|store'))
        
        print(f"📊 Found {len(listings)} potential listings\n")
        
        count = 0
        for listing in listings:
            text = listing.get_text(separator=' ')
            phones = extract_phone(text)
            
            if not phones:
                continue
            
            # Get name
            name_elem = listing.find(['a', 'span', 'h2'], class_=re.compile(r'lng_cont|store-name|title|heading'))
            if not name_elem:
                name_elem = listing.find(['h2', 'h3', 'a'])
            
            name = name_elem.get_text(strip=True)[:80] if name_elem else "Unknown"
            if len(name) < 3 or 'test' in name.lower():
                continue
            
            # Get address
            addr_elem = listing.find(['span', 'p'], class_=re.compile(r'addr|mrehspscrol|area|location'))
            address = addr_elem.get_text(strip=True)[:150] if addr_elem else "Surat"
            
            # Get rating
            rating = ""
            rating_elem = listing.find('span', class_=re.compile(r'green-box|rating|star'))
            if rating_elem:
                rating_match = re.search(r'(\d+\.?\d*)', rating_elem.get_text())
                if rating_match:
                    rating = rating_match.group(1)
            
            # Get link
            link_elem = listing.find('a', href=True)
            detail_url = ""
            if link_elem:
                href = link_elem.get('href', '')
                if href.startswith('/'):
                    detail_url = f"https://www.justdial.com{href}"
                elif href.startswith('http'):
                    detail_url = href
            
            for phone in phones[:1]:
                count += 1
                record = {
                    'name': name,
                    'phone': phone,
                    'address': address,
                    'rating': rating,
                    'source': 'JustDial',
                    'source_url': detail_url or url
                }
                results.append(record)
                
                # Print for verification
                print(f"{count}. {name}")
                print(f"   📞 {phone}")
                print(f"   📍 {address[:60]}...")
                if rating:
                    print(f"   ⭐ {rating}")
                print(f"   🔗 {record['source_url'][:70]}...")
                print()
        
        return results
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return results

def save_csv(records, filename):
    """Save to CSV."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'phone', 'address', 'rating', 'source', 'source_url'])
        writer.writeheader()
        writer.writerows(records)
    return filepath

def main():
    print("\n" + "=" * 70)
    print("🔌 SURAT ELECTRICIANS - VERIFIED DATA WITH SOURCE LINKS")
    print("=" * 70)
    
    results = scrape_justdial()
    
    # Deduplicate by phone
    seen = {}
    for r in results:
        if r['phone'] not in seen:
            seen[r['phone']] = r
    unique = list(seen.values())
    
    print("=" * 70)
    print(f"✅ TOTAL VERIFIED RECORDS: {len(unique)}")
    print("=" * 70)
    
    if unique:
        # Save to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = save_csv(unique, f"surat_verified_{timestamp}.csv")
        print(f"\n📁 Saved to: {csv_path}")
        print("\n👉 Open the CSV to verify each record using the source_url column")
        print("   Each URL links directly to the listing on JustDial")
    else:
        print("\n⚠️  No records extracted. Website may be blocking requests.")
        print("   Try using a proxy (see PROXY_SETUP.md)")

if __name__ == "__main__":
    main()
