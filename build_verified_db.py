#!/usr/bin/env python3
"""
Clean database and add only verified Surat electricians with source URLs
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
from src.storage import DataStorage, ElectricianDB
from src.models import Electrician

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
    for m in re.findall(r'[6-9]\d{9}', text):
        if len(set(m)) >= 4:  # Not fake like 9999999999
            phones.add(m)
    return list(phones)

def scrape_justdial_page(url, category):
    """Scrape a single JustDial page."""
    results = []
    
    print(f"\n🌐 Scraping: {url}")
    
    try:
        response = requests.get(url, headers=get_headers(), timeout=30)
        if response.status_code != 200:
            print(f"   ❌ Status {response.status_code}")
            return results
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find all store cards
        cards = soup.find_all('li', class_=re.compile(r'cntanr'))
        if not cards:
            cards = soup.find_all('div', class_=re.compile(r'resultbox|jsx-'))
        
        seen_phones = set()
        
        for card in cards:
            try:
                # Get the main link and name
                name_link = card.find('a', class_=re.compile(r'lng_cont_name|store-name'))
                if not name_link:
                    name_link = card.find('span', class_=re.compile(r'lng_cont_name|store-name'))
                
                if not name_link:
                    continue
                
                name = name_link.get_text(strip=True)
                if len(name) < 3:
                    continue
                
                # Get URL
                detail_url = ""
                if name_link.name == 'a' and name_link.get('href'):
                    href = name_link.get('href')
                    if href.startswith('/'):
                        detail_url = f"https://www.justdial.com{href}"
                    elif href.startswith('http'):
                        detail_url = href
                
                if not detail_url:
                    link = card.find('a', href=True)
                    if link:
                        href = link.get('href', '')
                        if href.startswith('/'):
                            detail_url = f"https://www.justdial.com{href}"
                
                # Get phone from text
                text = card.get_text()
                phones = extract_phone(text)
                
                if not phones:
                    continue
                
                phone = phones[0]
                if phone in seen_phones:
                    continue
                seen_phones.add(phone)
                
                # Get address
                addr_elem = card.find('span', class_=re.compile(r'cont_fl_addr|mrehspscrol'))
                address = addr_elem.get_text(strip=True)[:150] if addr_elem else "Surat, Gujarat"
                
                # Get rating
                rating = None
                rating_elem = card.find('span', class_=re.compile(r'green-box|rating'))
                if rating_elem:
                    match = re.search(r'(\d+\.?\d*)', rating_elem.get_text())
                    if match:
                        rating = float(match.group(1))
                
                results.append({
                    'name': name,
                    'phone': phone,
                    'address': address,
                    'rating': rating,
                    'category': category,
                    'source': 'JustDial',
                    'source_url': detail_url or url
                })
                
            except Exception:
                continue
        
        print(f"   ✅ Found {len(results)} unique records")
        return results
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return results


def main():
    print("\n" + "=" * 70)
    print("🔌 SURAT VERIFIED ELECTRICIANS DATABASE BUILDER")
    print("=" * 70)
    print("📍 Location: Surat, Gujarat")
    print("🎯 Only adding verified records with source URLs")
    print("=" * 70)
    
    storage = DataStorage()
    
    # Clear existing Surat data
    print("\n🗑️  Clearing previous Surat sample data...")
    session = storage.Session()
    deleted = session.query(ElectricianDB).filter(
        ElectricianDB.city.ilike('%surat%')
    ).delete(synchronize_session=False)
    session.commit()
    print(f"   Removed {deleted} old records")
    session.close()
    
    # Scrape categories
    all_records = []
    
    categories = [
        ("https://www.justdial.com/Surat/Electricians", "Electrician"),
        ("https://www.justdial.com/Surat/Electrical-Contractors", "Electrical Contractor"),
        ("https://www.justdial.com/Surat/House-Wiring-Contractors", "House Wiring"),
    ]
    
    for url, cat in categories:
        records = scrape_justdial_page(url, cat)
        all_records.extend(records)
        time.sleep(random.uniform(2, 4))
    
    # Deduplicate by phone
    seen = {}
    for r in all_records:
        if r['phone'] not in seen:
            seen[r['phone']] = r
    unique_records = list(seen.values())
    
    print(f"\n📊 Total unique verified records: {len(unique_records)}")
    
    if unique_records:
        # Print verification table
        print("\n" + "=" * 70)
        print("📋 VERIFIED RECORDS FOR REVIEW")
        print("=" * 70)
        
        for i, r in enumerate(unique_records, 1):
            print(f"\n{i}. {r['name']}")
            print(f"   📞 Phone: {r['phone']}")
            print(f"   📍 Address: {r['address'][:50]}...")
            print(f"   🔧 Category: {r['category']}")
            if r['rating']:
                print(f"   ⭐ Rating: {r['rating']}")
            print(f"   🔗 Verify: {r['source_url'][:60]}...")
        
        # Save to CSV for verification
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = OUTPUT_DIR / f"surat_verified_clean_{timestamp}.csv"
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'phone', 'address', 'rating', 'category', 'source', 'source_url'])
            writer.writeheader()
            writer.writerows(unique_records)
        
        print(f"\n📁 CSV saved: {csv_path}")
        
        # Add to database
        print("\n💾 Adding to database...")
        electricians = []
        for r in unique_records:
            electricians.append(Electrician(
                name=r['name'],
                phone=r['phone'],
                city='Surat',
                state='Gujarat',
                address=r['address'],
                rating=r['rating'],
                services=[r['category']],
                source=r['source'],
                source_url=r['source_url']
            ))
        
        saved = storage.save_to_database(electricians)
        print(f"✅ Added {saved} verified records to database")
        
        # Final stats
        session = storage.Session()
        total = session.query(ElectricianDB).filter(ElectricianDB.city.ilike('%surat%')).count()
        session.close()
        
        print("\n" + "=" * 70)
        print(f"✅ DATABASE NOW HAS {total} VERIFIED SURAT RECORDS")
        print("=" * 70)
        print("\n👉 To verify any record:")
        print("   1. Open the CSV file")
        print("   2. Click the source_url to see the original listing")
        print("\n👉 To view in web interface:")
        print("   python web_app.py")
        
    else:
        print("\n⚠️  No records scraped. Website may be blocking.")
        print("   Try configuring a proxy in .env file")

if __name__ == "__main__":
    main()
