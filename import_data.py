#!/usr/bin/env python3
"""
Import data from JSON export to database.
Usage: python import_data.py <json_file>
"""
import sys
import json
from datetime import datetime
from src.storage import DataStorage, ElectricianDB

def import_from_json(json_file):
    """Import records from JSON file."""
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    storage = DataStorage()
    session = storage.Session()
    
    added = 0
    skipped = 0
    
    for record in data:
        # Check if record already exists
        existing = session.query(ElectricianDB).filter(
            ElectricianDB.unique_key == record.get('unique_key')
        ).first()
        
        if existing:
            skipped += 1
            continue
        
        # Create new record
        new_record = ElectricianDB(
            name=record.get('name'),
            phone=record.get('phone'),
            city=record.get('city'),
            state=record.get('state'),
            address=record.get('address'),
            rating=record.get('rating'),
            review_count=record.get('review_count'),
            source=record.get('source'),
            source_url=record.get('source_url'),
            verified=record.get('verified', False),
            category=record.get('category'),
            service_description=record.get('service_description'),
            smart_meter_score=record.get('smart_meter_score', 0),
            unique_key=record.get('unique_key'),
            scraped_at=datetime.now()
        )
        
        session.add(new_record)
        added += 1
    
    session.commit()
    session.close()
    
    print(f'Import complete: {added} added, {skipped} skipped')
    return added, skipped

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python import_data.py <json_file>')
        sys.exit(1)
    
    import_from_json(sys.argv[1])
