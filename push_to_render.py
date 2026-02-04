#!/usr/bin/env python3
"""
Push local database records to Render's production database via API.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
import json
from src.storage import DataStorage, ElectricianDB

RENDER_URL = "https://india-electricians-db-1.onrender.com"

def push_to_render():
    print("="*60)
    print("🚀 Pushing local data to Render")
    print("="*60)
    
    storage = DataStorage()
    session = storage.Session()
    
    # Get all UP records from local database
    records = session.query(ElectricianDB).filter(
        ElectricianDB.state == 'Uttar Pradesh'
    ).all()
    
    print(f"\n📊 Found {len(records)} records in local database for UP")
    
    # Convert to JSON-serializable format
    data = []
    for r in records:
        data.append({
            'name': r.name,
            'phone': r.phone,
            'city': r.city,
            'state': r.state,
            'address': r.address,
            'rating': r.rating,
            'review_count': r.review_count,
            'website': r.website or '',
            'source': r.source,
            'category': r.category or '',
            'verified': r.verified or False,
        })
    
    session.close()
    
    # Push in batches
    batch_size = 100
    total_imported = 0
    total_skipped = 0
    
    print(f"\n📤 Pushing to {RENDER_URL}...")
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        print(f"  Batch {i//batch_size + 1}: {len(batch)} records...", end=" ")
        
        try:
            response = requests.post(
                f"{RENDER_URL}/api/import",
                json={'records': batch},
                headers={'Content-Type': 'application/json'},
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                total_imported += result.get('imported', 0)
                total_skipped += result.get('skipped', 0)
                print(f"✅ Imported: {result.get('imported', 0)}, Skipped: {result.get('skipped', 0)}")
            else:
                print(f"❌ Error: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("\n" + "="*60)
    print("📊 IMPORT COMPLETE")
    print("="*60)
    print(f"✅ Total imported: {total_imported}")
    print(f"⏭️  Total skipped (duplicates): {total_skipped}")
    print(f"\n🌐 Check: {RENDER_URL}")


if __name__ == "__main__":
    push_to_render()
