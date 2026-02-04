#!/usr/bin/env python3
"""
Google Places API Scraper for Kakod Area Electricians
Uses the official Google Places API for verified business data
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
import time
import csv
import json
from datetime import datetime
from src.storage import DataStorage, ElectricianDB
from src.models import Electrician

# Load API key
API_KEY = "AIzaSyCO2208ZcMxOkMnCq9qRRqx0x-CiXxgabg"

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def search_google_places(query: str, location: str):
    """Search Google Places API for businesses."""
    results = []
    
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
                    "location": place.get("geometry", {}).get("location", {})
                }
                results.append(result)
            
            # Handle pagination
            next_token = data.get("next_page_token")
            page = 2
            while next_token and page <= 3:
                time.sleep(2)  # Required delay for next_page_token
                params["pagetoken"] = next_token
                if "query" in params:
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
                            "location": place.get("geometry", {}).get("location", {})
                        }
                        results.append(result)
                    print(f"   ✅ Found {len(data.get('results', []))} more results (page {page})")
                    next_token = data.get("next_page_token")
                    page += 1
                else:
                    break
        
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
    print("🔌 GOOGLE PLACES API - KAKOD AREA ELECTRICIANS SCRAPER")
    print("=" * 70)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("🎯 Using Google Places API for verified business data")
    print("=" * 70)
    
    storage = DataStorage()
    
    # Search locations - Kakod and nearby areas
    locations = [
        ("Kakod, Uttar Pradesh, India", "Kakod"),
        ("Jewar, Uttar Pradesh, India", "Jewar"),
        ("Greater Noida, Uttar Pradesh, India", "Greater Noida"),
        ("Dadri, Uttar Pradesh, India", "Dadri"),
    ]
    
    # Search queries for different services
    search_queries = [
        ("electrician", "Electrician"),
        ("electrical contractor", "Electrical Contractor"),
        ("electrical services", "Electrical Services"),
        ("bijli mistri", "Bijli Mistri"),
        ("house wiring electrician", "House Wiring"),
        ("electrical repair", "Electrical Repair"),
    ]
    
    all_places = []
    seen_place_ids = set()
    
    print("\n📍 Searching Google Places API for Kakod area...")
    
    for location, city_name in locations:
        print(f"\n{'='*50}")
        print(f"📍 Location: {city_name}")
        print(f"{'='*50}")
        
        for query, service_type in search_queries:
            places = search_google_places(query, location)
            
            for place in places:
                if place["place_id"] not in seen_place_ids:
                    place["service_type"] = service_type
                    place["city"] = city_name
                    all_places.append(place)
                    seen_place_ids.add(place["place_id"])
            
            time.sleep(1)  # Rate limiting
    
    print(f"\n📊 Found {len(all_places)} unique businesses total")
    
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
                "city": place["city"],
                "state": "Uttar Pradesh",
                "rating": details.get("rating", place.get("rating")),
                "reviews": details.get("user_ratings_total", place.get("reviews")),
                "website": details.get("website", ""),
                "google_url": details.get("url", ""),
                "service_type": place["service_type"],
                "source": "google_places",
                "verified": True,
            }
            
            if phone:
                print(f"📞 {phone}")
                verified_records.append(record)
            else:
                print("⚠️ No phone")
        else:
            print("❌ Details failed")
        
        time.sleep(0.5)  # Rate limiting
    
    print(f"\n✅ Got {len(verified_records)} records with phone numbers")
    
    # Save to database
    if verified_records:
        electricians = []
        for r in verified_records:
            electricians.append(Electrician(
                name=r["name"],
                phone=r["phone"],
                city=r["city"],
                state=r["state"],
                address=r["address"],
                rating=r["rating"],
                review_count=r["reviews"],
                website=r["website"],
                source="google_places",
                verified=True,
            ))
        
        saved = storage.save_to_database(electricians)
        print(f"💾 Saved {saved} new records to database")
        
        # Export to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = OUTPUT_DIR / f"kakod_google_places_{timestamp}.csv"
        
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "phone", "city", "state", "address", "rating", "reviews", "website", "service_type", "source"])
            writer.writeheader()
            writer.writerows(verified_records)
        
        print(f"📄 Exported to: {csv_file}")
        
        # Export to JSON
        json_file = OUTPUT_DIR / f"kakod_google_places_{timestamp}.json"
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(verified_records, f, indent=2, ensure_ascii=False)
        print(f"📄 Exported to: {json_file}")
        
        # Print summary
        print("\n" + "=" * 70)
        print("📊 RESULTS SUMMARY")
        print("=" * 70)
        
        by_city = {}
        for r in verified_records:
            city = r["city"]
            by_city[city] = by_city.get(city, 0) + 1
        
        for city, count in sorted(by_city.items()):
            print(f"   {city}: {count} electricians")
        
        print("\n📋 Sample Records:")
        print("-" * 70)
        for r in verified_records[:10]:
            print(f"   {r['name']}")
            print(f"   📞 {r['phone']} | ⭐ {r.get('rating', 'N/A')} | 📍 {r['city']}")
            print()
    
    print("\n🎉 Google Places scraping complete!")


if __name__ == "__main__":
    main()
