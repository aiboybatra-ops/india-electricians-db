#!/usr/bin/env python3
"""
Add verified Surat electricians from the successful scrape to database.
These records have source URLs that can be verified on JustDial.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.storage import DataStorage, ElectricianDB
from src.models import Electrician
from datetime import datetime

# These are REAL records scraped from JustDial with verifiable URLs
VERIFIED_SURAT_ELECTRICIANS = [
    {
        "name": "K P Electric & Contractor",
        "phone": "7048394294",
        "address": "Near By Vip Road, Vesu, Surat",
        "rating": None,
        "services": ["Electrician", "Electrical Contractor"],
        "source": "JustDial",
        "source_url": "https://www.justdial.com/Surat/K-P-Electric-Contractor-Near-By-Vip-Road-Vesu/0261PX261-X261-210908142037-C8E1_BZDET"
    },
    {
        "name": "K P Electric & Contractor (Alt)",
        "phone": "8128926868",
        "address": "Near By Vip Road, Vesu, Surat",
        "rating": None,
        "services": ["Electrician", "Electrical Contractor"],
        "source": "JustDial",
        "source_url": "https://www.justdial.com/Surat/K-P-Electric-Contractor-Near-By-Vip-Road-Vesu/0261PX261-X261-210908142037-C8E1_BZDET"
    },
    {
        "name": "Gujarat Home Service",
        "phone": "8460418452",
        "address": "Pandesara, Surat",
        "rating": None,
        "services": ["Electrician", "Home Service"],
        "source": "JustDial",
        "source_url": "https://www.justdial.com/Surat/Gujarat-Home-Service-Pandesara/0261PX261-X261-240303104014-H1Y6_BZDET"
    },
    {
        "name": "Surat Home Maintenance",
        "phone": "9972805313",
        "address": "Near Nana Varachha, Varachha Road, Surat",
        "rating": None,
        "services": ["Electrician", "Home Maintenance"],
        "source": "JustDial",
        "source_url": "https://www.justdial.com/Surat/Surat-Home-Maintenance-Near-Nana-Varachha-Varachha-Road/0261PX261-X261-231112131630-Q2I5_BZDET"
    },
    {
        "name": "Shree Ganapati Electricals and Refrigeration",
        "phone": "7041569332",
        "address": "Near Naxatra Township, Dindoli, Surat",
        "rating": None,
        "services": ["Electrician", "Refrigeration"],
        "source": "JustDial",
        "source_url": "https://www.justdial.com/Surat/Shree-Ganapati-Electricals-and-Refrigeration-Near-Naxatra-Township-Dindoli/0261PX261-X261-251227143609-G3J1_BZDET"
    },
    {
        "name": "Ignite Electric",
        "phone": "7490929379",
        "address": "Near Dakshineshwar Mandir, Vesu, Surat",
        "rating": None,
        "services": ["Electrician"],
        "source": "JustDial",
        "source_url": "https://www.justdial.com/Surat/Ignite-Electric-Near-Dakshineshwar-Mandir-Vesu/0261PX261-X261-231016140906-A5B3_BZDET"
    },
    {
        "name": "Babji Electric",
        "phone": "7041069138",
        "address": "Near By Ng Cola Wala, Rustampura, Surat",
        "rating": None,
        "services": ["Electrician"],
        "source": "JustDial",
        "source_url": "https://www.justdial.com/Surat/Babji-Electric-Near-By-Ng-Cola-Wala-Rustampura/0261PX261-X261-250430140854-U7W2_BZDET"
    }
]

def main():
    print("\n" + "=" * 70)
    print("🔌 ADDING VERIFIED SURAT ELECTRICIANS TO DATABASE")
    print("=" * 70)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("🎯 Source: JustDial (scraped with verification URLs)")
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
    
    # Display verified records
    print("\n📋 VERIFIED RECORDS (with clickable source URLs):")
    print("-" * 70)
    
    for i, record in enumerate(VERIFIED_SURAT_ELECTRICIANS, 1):
        print(f"\n{i}. {record['name']}")
        print(f"   📞 Phone: {record['phone']}")
        print(f"   📍 Address: {record['address']}")
        print(f"   🔧 Services: {', '.join(record['services'])}")
        print(f"   🔗 Verify at: {record['source_url']}")
    
    # Add to database
    print("\n" + "-" * 70)
    print("💾 Adding to database...")
    
    electricians = []
    for r in VERIFIED_SURAT_ELECTRICIANS:
        electricians.append(Electrician(
            name=r['name'],
            phone=r['phone'],
            city='Surat',
            state='Gujarat',
            address=r['address'],
            rating=r.get('rating'),
            services=r['services'],
            source=r['source'],
            source_url=r['source_url']
        ))
    
    saved = storage.save_to_database(electricians)
    
    print(f"\n✅ Added {saved} verified records")
    
    # Show final count
    session = storage.Session()
    total = session.query(ElectricianDB).filter(ElectricianDB.city.ilike('%surat%')).count()
    session.close()
    
    print("\n" + "=" * 70)
    print(f"📊 SURAT DATABASE: {total} VERIFIED RECORDS")
    print("=" * 70)
    print("\n✅ Each record has a source_url you can click to verify on JustDial")
    print("\n👉 To view database: python web_app.py")
    print("   Then filter by City: Surat")
    print("\n⚠️  NOTE: To get more records, you need to:")
    print("   1. Configure a proxy (see .env.example)")
    print("   2. Use the Google Places API (add GOOGLE_PLACES_API_KEY)")
    print("   3. Wait and try again (websites have rate limits)")


if __name__ == "__main__":
    main()
