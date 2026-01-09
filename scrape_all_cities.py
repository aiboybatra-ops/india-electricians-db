#!/usr/bin/env python3
"""
Automated scraper for multiple cities - can be run via cron job or scheduler.
Usage: python scrape_all_cities.py [--cities "City1,City2"] [--state "State"]
"""

import requests
import time
import argparse
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.storage import DataStorage, ElectricianDB

API_KEY = os.getenv('GOOGLE_PLACES_API_KEY', 'AIzaSyCO2208ZcMxOkMnCq9qRRqx0x-CiXxgabg')

# City coordinates (lat, lng)
CITY_COORDS = {
    'Surat': ('21.1702', '72.8311', 'Gujarat'),
    'Bhopal': ('23.2599', '77.4126', 'Madhya Pradesh'),
    'Ahmedabad': ('23.0225', '72.5714', 'Gujarat'),
    'Vadodara': ('22.3072', '73.1812', 'Gujarat'),
    'Rajkot': ('22.3039', '70.8022', 'Gujarat'),
    'Indore': ('22.7196', '75.8577', 'Madhya Pradesh'),
    'Mumbai': ('19.0760', '72.8777', 'Maharashtra'),
    'Pune': ('18.5204', '73.8567', 'Maharashtra'),
    'Delhi': ('28.7041', '77.1025', 'Delhi'),
    'Bangalore': ('12.9716', '77.5946', 'Karnataka'),
    'Chennai': ('13.0827', '80.2707', 'Tamil Nadu'),
    'Hyderabad': ('17.3850', '78.4867', 'Telangana'),
    'Kolkata': ('22.5726', '88.3639', 'West Bengal'),
    'Jaipur': ('26.9124', '75.7873', 'Rajasthan'),
    'Lucknow': ('26.8467', '80.9462', 'Uttar Pradesh'),
}


def search_places(query, location, radius=25000):
    """Search Google Places API."""
    url = 'https://maps.googleapis.com/maps/api/place/textsearch/json'
    all_results = []
    
    params = {
        'query': query,
        'location': location,
        'radius': radius,
        'key': API_KEY
    }
    
    while True:
        response = requests.get(url, params=params)
        data = response.json()
        
        if data.get('status') != 'OK':
            break
        
        all_results.extend(data.get('results', []))
        
        next_token = data.get('next_page_token')
        if not next_token:
            break
        
        time.sleep(2)
        params = {'pagetoken': next_token, 'key': API_KEY}
    
    return all_results


def get_place_details(place_id):
    """Get detailed info including phone number."""
    url = 'https://maps.googleapis.com/maps/api/place/details/json'
    params = {
        'place_id': place_id,
        'fields': 'formatted_phone_number,international_phone_number,website',
        'key': API_KEY
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if data.get('status') == 'OK':
        return data.get('result', {})
    return {}


def categorize_business(name):
    """Categorize business and assign smart meter score."""
    name_lower = name.lower() if name else ''
    
    if any(word in name_lower for word in ['meter', 'metering', 'prepaid']):
        return 'Meter Installer', 'Meter installation services', 95
    elif any(word in name_lower for word in ['contractor', 'contracting']):
        return 'Electrical Contractor', 'Electrical contracting services', 80
    elif any(word in name_lower for word in ['industrial', 'factory']):
        return 'Industrial Electrician', 'Industrial electrical services', 75
    elif any(word in name_lower for word in ['solar', 'panel']):
        return 'Solar Installation', 'Solar installation services', 70
    elif any(word in name_lower for word in ['ac ', 'air condition']):
        return 'AC Electrician', 'AC electrical services', 45
    elif any(word in name_lower for word in ['house', 'home', 'wiring']):
        return 'House Wiring', 'Residential wiring services', 55
    else:
        return 'Electrician', 'General electrical services', 50


def scrape_city(city, state, lat, lng, session):
    """Scrape electricians for a single city."""
    print(f'\n{"="*60}')
    print(f'Scraping: {city}, {state}')
    print(f'{"="*60}')
    
    queries = [
        f'electricians in {city}',
        f'electrical contractors {city}',
        f'meter installation {city}',
        f'electrical services {city}',
        f'house wiring {city}',
        f'industrial electrician {city}',
        f'solar installation {city}'
    ]
    
    location = f'{lat},{lng}'
    all_places = {}
    
    for query in queries:
        print(f'  Searching: {query}')
        results = search_places(query, location)
        for place in results:
            place_id = place.get('place_id')
            if place_id and place_id not in all_places:
                all_places[place_id] = place
    
    print(f'  Found {len(all_places)} unique places')
    
    count = 0
    for place_id, place in all_places.items():
        name = place.get('name', '')
        address = place.get('formatted_address', '')
        
        details = get_place_details(place_id)
        phone = details.get('formatted_phone_number', '') or details.get('international_phone_number', '')
        phone = phone.replace(' ', '').replace('-', '').replace('+91', '')
        
        if not phone:
            continue
        
        # Check for duplicates
        existing = session.query(ElectricianDB).filter(
            ElectricianDB.phone == phone
        ).first()
        
        if existing:
            continue
        
        category, description, score = categorize_business(name)
        
        record = ElectricianDB(
            name=name,
            phone=phone,
            city=city,
            state=state,
            address=address,
            rating=place.get('rating'),
            review_count=place.get('user_ratings_total'),
            source='Google Places',
            source_url=f'https://www.google.com/maps/place/?q=place_id:{place_id}',
            verified=True,
            verified_by='Google Places API',
            verified_at=datetime.now(),
            scraped_at=datetime.now(),
            unique_key=f'google_{city}_{name}_{phone}',
            category=category,
            service_description=description,
            smart_meter_score=score
        )
        
        session.add(record)
        count += 1
        time.sleep(0.1)
    
    session.commit()
    print(f'  ✓ Added {count} new records for {city}')
    return count


def main():
    parser = argparse.ArgumentParser(description='Scrape electricians from multiple cities')
    parser.add_argument('--cities', type=str, help='Comma-separated list of cities to scrape')
    parser.add_argument('--all', action='store_true', help='Scrape all configured cities')
    args = parser.parse_args()
    
    storage = DataStorage()
    session = storage.Session()
    
    # Determine which cities to scrape
    if args.all:
        cities_to_scrape = list(CITY_COORDS.keys())
    elif args.cities:
        cities_to_scrape = [c.strip() for c in args.cities.split(',')]
    else:
        # Default: scrape cities not yet in database or with fewer records
        from sqlalchemy import func
        existing = session.query(ElectricianDB.city, func.count(ElectricianDB.id)).group_by(ElectricianDB.city).all()
        existing_cities = {c: cnt for c, cnt in existing}
        cities_to_scrape = [c for c in CITY_COORDS.keys() if existing_cities.get(c, 0) < 50]
    
    print('='*60)
    print('Electricians Database - Automated Scraper')
    print(f'Started: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*60)
    print(f'Cities to scrape: {", ".join(cities_to_scrape)}')
    
    total_added = 0
    for city in cities_to_scrape:
        if city in CITY_COORDS:
            lat, lng, state = CITY_COORDS[city]
            count = scrape_city(city, state, lat, lng, session)
            total_added += count
        else:
            print(f'  ⚠ City "{city}" not found in coordinates database')
    
    # Print summary
    from sqlalchemy import func
    print('\n' + '='*60)
    print('SCRAPING COMPLETE - Summary')
    print('='*60)
    print(f'New records added: {total_added}')
    
    by_city = session.query(
        ElectricianDB.city,
        func.count(ElectricianDB.id)
    ).group_by(ElectricianDB.city).order_by(func.count(ElectricianDB.id).desc()).all()
    
    print('\nRecords by city:')
    for city, cnt in by_city:
        print(f'  {city}: {cnt}')
    
    total = session.query(ElectricianDB).count()
    print(f'\nTotal records: {total}')
    print(f'Completed: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    session.close()


if __name__ == '__main__':
    main()
