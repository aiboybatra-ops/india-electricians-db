#!/usr/bin/env python3
"""
Google Places API Scraper for Surat Electricians
Uses the official Google Places API for verified business data
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
import time
import csv
from datetime import datetime
from src.storage import DataStorage, ElectricianDB
from src.models import Electrician

# Load API key from .env.example or environment
API_KEY = "AIzaSyCO2208ZcMxOkMnCq9qRRqx0x-CiXxgabg"

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

def search_google_places(query: str, location: str = "Surat, Gujarat, India"):
    """Search Google Places API for businesses."""
    results = []
    
    # Text Search endpoint
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    
    params = {
        "query": f"{query} in {location}",
        "key": API_KEY,
        "language": "en",
        "region": "in"
    }
    
    print(f"\n🔍 Searching: {query} in {location}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if data.get("status") == "OK":
            places = data.get("results", [])
            print(f"   ✅ Found {len(places)} results")
            
            for place in places:
                result = {
                    "name": place.get("name", ""),
                    "address": place.get("formatted_address", ""),
                    "rating": place.get("rating"),
                    "reviews": place.get("user_ratings_total"),
                    "place_id": place.get("place_id"),
                    "types": place.get("types", []),
                    "business_status": place.get("business_status", ""),
                }
                results.append(result)
            
            # Handle pagination
            next_token = data.get("next_page_token")
            if next_token:
                time.sleep(2)  # Required delay for next_page_token
                params["pagetoken"] = next_token
                del params["query"]
                
                response = requests.get(url, params=params, timeout=30)
                data = response.json()
                
                if data.get("status") == "OK":
                    for place in data.get("results", []):
                        result = {
                            "name": place.get("name", ""),
                            "address": place.get("formatted_address", ""),
                            "rating": place.get("rating"),
                            "reviews": place.get("user_ratings_total"),
                            "place_id": place.get("place_id"),
                            "types": place.get("types", []),
                            "business_status": place.get("business_status", ""),
                        }
                        results.append(result)
                    print(f"   ✅ Found {len(data.get('results', []))} more results (page 2)")
        
        elif data.get("status") == "ZERO_RESULTS":
            print(f"   ⚠️  No results found")
        else:
            print(f"   ❌ API Error: {data.get('status')} - {data.get('error_message', '')}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    return results


def get_place_details(place_id: str):
    """Get detailed info including phone number."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    
    params = {
        "place_id": place_id,
        "key": API_KEY,
        "fields": "name,formatted_address,formatted_phone_number,international_phone_number,rating,user_ratings_total,website,url,business_status,types"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if data.get("status") == "OK":
            return data.get("result", {})
        else:
            return None
            
    except Exception as e:
        return None


def extract_phone_digits(phone: str) -> str:
    """Extract 10-digit Indian phone number."""
    if not phone:
        return ""
    digits = "".join(filter(str.isdigit, phone))
    if len(digits) >= 10:
        return digits[-10:]
    return ""


def main():
    print("\n" + "=" * 70)
    print("🔌 GOOGLE PLACES API - SURAT ELECTRICIANS SCRAPER")
    print("=" * 70)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("🎯 Using Google Places API for verified business data")
    print("=" * 70)
    
    storage = DataStorage()
    
    # Clear old Surat data
    print("\n🗑️  Clearing previous Surat data...")
    session = storage.Session()
    deleted = session.query(ElectricianDB).filter(
        ElectricianDB.city.ilike('%surat%')
    ).delete(synchronize_session=False)
    session.commit()
    session.close()
    print(f"   Removed {deleted} old records")
    
    # Search queries for different services
    search_queries = [
        ("electrician", "Electrician"),
        ("electrical contractor", "Electrical Contractor"),
        ("electrical services", "Electrical Services"),
        ("house wiring", "House Wiring"),
        ("meter installation", "Meter Installer"),
        ("electrical lineman", "Electrical Lineman"),
        ("industrial electrician", "Industrial Electrician"),
        ("AC electrician", "AC Electrician"),
        ("solar panel installation", "Solar Installation"),
    ]
    
    all_places = []
    seen_place_ids = set()
    
    print("\n📍 Searching Google Places API...")
    
    for query, service_type in search_queries:
        places = search_google_places(query, "Surat, Gujarat, India")
        
        for place in places:
            if place["place_id"] not in seen_place_ids:
                place["service_type"] = service_type
                all_places.append(place)
                seen_place_ids.add(place["place_id"])
        
        time.sleep(1)  # Rate limiting
    
    print(f"\n📊 Found {len(all_places)} unique businesses")
    
    if not all_places:
        print("\n⚠️  No results from Google Places API.")
        print("   Check if the API key is valid and has Places API enabled.")
        return
    
    # Get phone numbers for each place
    print("\n📞 Fetching phone numbers (this may take a moment)...")
    
    verified_records = []
    
    for i, place in enumerate(all_places, 1):
        print(f"   [{i}/{len(all_places)}] {place['name'][:40]}...", end=" ")
        
        details = get_place_details(place["place_id"])
        
        if details:
            phone = extract_phone_digits(details.get("formatted_phone_number") or details.get("international_phone_number"))
            
            record = {
                "name": details.get("name", place["name"]),
                "phone": phone,
                "address": details.get("formatted_address", place["address"]),
                "rating": details.get("rating", place.get("rating")),
                "reviews": details.get("user_ratings_total", place.get("reviews")),
                "website": details.get("website", ""),
                "google_url": details.get("url", ""),
                "service_type": place["service_type"],
                "source": "Google Places",
                "place_id": place["place_id"]
            }
            
            if phone:
                verified_records.append(record)
                print(f"✅ {phone}")
            else:
                print("⚠️  No phone")
        else:
            print("❌ Failed")
        
        time.sleep(0.5)  # Rate limiting
    
    # Deduplicate by phone
    seen_phones = {}
    unique_records = []
    for r in verified_records:
        if r["phone"] and r["phone"] not in seen_phones:
            seen_phones[r["phone"]] = True
            unique_records.append(r)
    
    print(f"\n📊 RESULTS: {len(unique_records)} unique verified records with phone numbers")
    
    if unique_records:
        # Display records
        print("\n" + "=" * 70)
        print("📋 VERIFIED SURAT ELECTRICIANS (from Google Places)")
        print("=" * 70)
        
        for i, r in enumerate(unique_records, 1):
            print(f"\n{i}. {r['name']}")
            print(f"   📞 Phone: {r['phone']}")
            print(f"   📍 {r['address'][:60]}...")
            print(f"   🔧 Service: {r['service_type']}")
            if r['rating']:
                print(f"   ⭐ Rating: {r['rating']} ({r['reviews']} reviews)")
            if r['website']:
                print(f"   🌐 Website: {r['website'][:50]}...")
            print(f"   🔗 Google Maps: {r['google_url']}")
        
        # Save to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = OUTPUT_DIR / f"surat_google_places_{timestamp}.csv"
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'name', 'phone', 'address', 'rating', 'reviews', 
                'service_type', 'website', 'google_url', 'source'
            ])
            writer.writeheader()
            for r in unique_records:
                writer.writerow({
                    'name': r['name'],
                    'phone': r['phone'],
                    'address': r['address'],
                    'rating': r['rating'] or '',
                    'reviews': r['reviews'] or '',
                    'service_type': r['service_type'],
                    'website': r['website'],
                    'google_url': r['google_url'],
                    'source': r['source']
                })
        
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
                review_count=r['reviews'],
                services=[r['service_type']],
                source=r['source'],
                source_url=r['google_url']
            ))
        
        saved = storage.save_to_database(electricians)
        print(f"✅ Added {saved} records to database")
        
        # Final stats
        session = storage.Session()
        total = session.query(ElectricianDB).filter(ElectricianDB.city.ilike('%surat%')).count()
        session.close()
        
        print("\n" + "=" * 70)
        print(f"✅ DATABASE NOW HAS {total} VERIFIED SURAT RECORDS")
        print("=" * 70)
        print("\n👉 Each record has a Google Maps URL for verification")
        print("👉 View database: python web_app.py → http://localhost:5000")
        
    else:
        print("\n⚠️  No records with phone numbers found.")
        print("   Many businesses on Google don't list phone numbers publicly.")


if __name__ == "__main__":
    main()
