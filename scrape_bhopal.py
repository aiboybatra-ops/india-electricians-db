#!/usr/bin/env python3
"""
Scrape Bhopal electricians from Google Places API.
"""

import requests
import time
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.storage import DataStorage, ElectricianDB

API_KEY = os.getenv('GOOGLE_PLACES_API_KEY', 'AIzaSyCO2208ZcMxOkMnCq9qRRqx0x-CiXxgabg')

def search_places(query, location='23.2599,77.4126', radius=25000):
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
            print(f'  API Status: {data.get("status")}')
            break
        
        all_results.extend(data.get('results', []))
        print(f'  Found {len(data.get("results", []))} results (total: {len(all_results)})')
        
        # Check for next page
        next_token = data.get('next_page_token')
        if not next_token:
            break
        
        time.sleep(2)  # Required delay for next_page_token
        params = {'pagetoken': next_token, 'key': API_KEY}
    
    return all_results


def get_place_details(place_id):
    """Get detailed info including phone number."""
    url = 'https://maps.googleapis.com/maps/api/place/details/json'
    params = {
        'place_id': place_id,
        'fields': 'formatted_phone_number,international_phone_number,website,opening_hours',
        'key': API_KEY
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if data.get('status') == 'OK':
        return data.get('result', {})
    return {}


def categorize_business(name):
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


def main():
    storage = DataStorage()
    session = storage.Session()
    
    # Search queries for Bhopal
    queries = [
        'electricians in Bhopal',
        'electrical contractors Bhopal',
        'meter installation Bhopal',
        'electrical services Bhopal',
        'house wiring Bhopal',
        'industrial electrician Bhopal',
        'solar installation Bhopal'
    ]
    
    print('=' * 60)
    print('Scraping Bhopal Electricians from Google Places API')
    print('=' * 60)
    
    all_places = {}
    for query in queries:
        print(f'\nSearching: {query}')
        results = search_places(query)
        for place in results:
            place_id = place.get('place_id')
            if place_id and place_id not in all_places:
                all_places[place_id] = place
    
    print(f'\nTotal unique places found: {len(all_places)}')
    
    # Get details and save to database
    count = 0
    for place_id, place in all_places.items():
        name = place.get('name', '')
        address = place.get('formatted_address', '')
        
        # Get phone number
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
        
        # Categorize
        category, description, score = categorize_business(name)
        
        # Create record
        record = ElectricianDB(
            name=name,
            phone=phone,
            city='Bhopal',
            state='Madhya Pradesh',
            address=address,
            rating=place.get('rating'),
            review_count=place.get('user_ratings_total'),
            source='Google Places',
            source_url=f'https://www.google.com/maps/place/?q=place_id:{place_id}',
            verified=True,
            verified_by='Google Places API',
            verified_at=datetime.now(),
            scraped_at=datetime.now(),
            unique_key=f'google_bhopal_{name}_{phone}',
            category=category,
            service_description=description,
            smart_meter_score=score
        )
        
        session.add(record)
        count += 1
        print(f'  Added: {name} ({phone})')
        
        time.sleep(0.1)  # Rate limiting
    
    session.commit()
    print(f'\n✓ Added {count} verified Bhopal electricians')
    
    # Show totals
    from sqlalchemy import func
    print('\n' + '=' * 60)
    print('Database Summary')
    print('=' * 60)
    
    by_city = session.query(
        ElectricianDB.city,
        func.count(ElectricianDB.id)
    ).group_by(ElectricianDB.city).all()
    
    for city, cnt in by_city:
        print(f'  {city}: {cnt} records')
    
    print(f'\nTotal: {session.query(ElectricianDB).count()} verified records')
    session.close()


if __name__ == '__main__':
    main()
