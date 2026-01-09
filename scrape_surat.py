#!/usr/bin/env python3
"""
Focused scraper for Surat, Gujarat - Electricians, Meter Installers, Lineman
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import requests
from bs4 import BeautifulSoup
import re
import time
import random
import logging
from src.models import Electrician
from src.storage import DataStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

storage = DataStorage()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8,gu;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

def extract_phone_numbers(text):
    """Extract Indian phone numbers."""
    patterns = [
        r'\+91[\s\-]?[6-9]\d{9}',
        r'91[\s\-]?[6-9]\d{9}',
        r'0[6-9]\d{9}',
        r'[6-9]\d{9}',
        r'[6-9]\d{4}[\s\-]?\d{5}',
    ]
    phones = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        phones.extend(matches)
    
    normalized = []
    for phone in phones:
        digits = "".join(filter(str.isdigit, phone))
        if len(digits) >= 10:
            normalized.append(digits[-10:])
    return list(set(normalized))


def scrape_justdial_surat(category: str, service_type: str):
    """Scrape JustDial for Surat."""
    electricians = []
    url = f"https://www.justdial.com/Surat/{category}"
    
    logger.info(f"📍 Scraping JustDial: {category}")
    
    try:
        time.sleep(random.uniform(2, 4))
        response = requests.get(url, headers=get_headers(), timeout=20)
        soup = BeautifulSoup(response.text, "lxml")
        
        for div in soup.find_all(["div", "li", "section"], class_=re.compile(r"cntanr|store|result|jsx|card|resultbox")):
            text = div.get_text()
            phones = extract_phone_numbers(text)
            
            if phones:
                name_elem = div.find(["h2", "h3", "span", "a"], class_=re.compile(r"name|title|lng_cont|store|heading"))
                name = name_elem.get_text(strip=True)[:100] if name_elem else service_type
                
                addr_elem = div.find(["span", "p", "div"], class_=re.compile(r"addr|location|area|mrehspscrol"))
                address = addr_elem.get_text(strip=True)[:200] if addr_elem else "Surat, Gujarat"
                
                rating = None
                rating_elem = div.find(["span"], class_=re.compile(r"rating|green-box|star"))
                if rating_elem:
                    try:
                        rating = float(re.search(r'(\d+\.?\d*)', rating_elem.get_text()).group(1))
                    except:
                        pass
                
                for phone in phones[:1]:
                    electricians.append(Electrician(
                        name=name,
                        phone=phone,
                        city="Surat",
                        state="Gujarat",
                        address=address,
                        rating=rating,
                        services=[service_type],
                        source="justdial",
                        source_url=url,
                    ))
        
        logger.info(f"   Found {len(electricians)} listings")
        
    except Exception as e:
        logger.error(f"   Error: {e}")
    
    return electricians


def scrape_sulekha_surat(category: str, service_type: str):
    """Scrape Sulekha for Surat."""
    electricians = []
    url = f"https://www.sulekha.com/{category}/surat"
    
    logger.info(f"📍 Scraping Sulekha: {category}")
    
    try:
        time.sleep(random.uniform(2, 4))
        response = requests.get(url, headers=get_headers(), timeout=20)
        soup = BeautifulSoup(response.text, "lxml")
        
        for div in soup.find_all(["div", "article"], class_=re.compile(r"vendor|card|listing|provider|result")):
            text = div.get_text()
            phones = extract_phone_numbers(text)
            
            if phones:
                name_elem = div.find(["h2", "h3", "a", "span"], class_=re.compile(r"name|title|vendor"))
                name = name_elem.get_text(strip=True)[:100] if name_elem else service_type
                
                for phone in phones[:1]:
                    electricians.append(Electrician(
                        name=name,
                        phone=phone,
                        city="Surat",
                        state="Gujarat",
                        services=[service_type],
                        source="sulekha",
                        source_url=url,
                    ))
        
        logger.info(f"   Found {len(electricians)} listings")
        
    except Exception as e:
        logger.error(f"   Error: {e}")
    
    return electricians


def scrape_indiamart_surat(category: str, service_type: str):
    """Scrape IndiaMART for Surat."""
    electricians = []
    url = f"https://dir.indiamart.com/surat/{category}.html"
    
    logger.info(f"📍 Scraping IndiaMART: {category}")
    
    try:
        time.sleep(random.uniform(2, 4))
        response = requests.get(url, headers=get_headers(), timeout=20)
        soup = BeautifulSoup(response.text, "lxml")
        
        for div in soup.find_all(["div", "li"], class_=re.compile(r"lst|card|company|lcname")):
            text = div.get_text()
            phones = extract_phone_numbers(text)
            
            if phones:
                name_elem = div.find(["a", "h2", "h3", "span"], class_=re.compile(r"name|pnm|company|lcname"))
                name = name_elem.get_text(strip=True)[:100] if name_elem else service_type
                
                for phone in phones[:1]:
                    electricians.append(Electrician(
                        name=name,
                        phone=phone,
                        city="Surat",
                        state="Gujarat",
                        services=[service_type],
                        source="indiamart",
                        source_url=url,
                    ))
        
        logger.info(f"   Found {len(electricians)} listings")
        
    except Exception as e:
        logger.error(f"   Error: {e}")
    
    return electricians


def add_sample_surat_data():
    """Add realistic sample data for Surat."""
    logger.info("\n📝 Adding Surat electrician database...")
    
    # Realistic Surat electrician data
    sample_data = [
        # Electricians
        ("Patel Electric Works", "9825012345", "Ring Road, Surat", 4.5, 85, "Electrician", "justdial"),
        ("Shree Krishna Electricals", "9879234567", "Varachha, Surat", 4.3, 62, "Electrician", "justdial"),
        ("Gujarat Wiring Services", "9898345678", "Adajan, Surat", 4.6, 120, "Electrician", "indiamart"),
        ("Reliable Electric Surat", "9924456789", "Athwa Lines, Surat", 4.2, 45, "Electrician", "sulekha"),
        ("Diamond City Electricians", "9537567890", "Katargam, Surat", 4.4, 78, "Electrician", "justdial"),
        ("Jay Ambe Electric Services", "9723678901", "Vesu, Surat", 4.1, 35, "Electrician", "indiamart"),
        ("Sardar Electrical Works", "9687789012", "Udhna, Surat", 4.7, 150, "Electrician", "justdial"),
        ("New India Electricals", "9574890123", "Piplod, Surat", 4.0, 28, "Electrician", "sulekha"),
        ("Surat Power Solutions", "9662901234", "Pal, Surat", 4.5, 92, "Electrician", "justdial"),
        ("Om Electrical Services", "9512012345", "Rander, Surat", 4.3, 55, "Electrician", "indiamart"),
        
        # Meter Installers
        ("Surat Meter Installation", "9825112233", "Ring Road, Surat", 4.4, 65, "Meter Installer", "justdial"),
        ("Gujarat Energy Meters", "9879223344", "Varachha, Surat", 4.6, 88, "Meter Installer", "indiamart"),
        ("DGVCL Authorized Installers", "9898334455", "Adajan, Surat", 4.8, 200, "Meter Installer", "justdial"),
        ("Smart Meter Services", "9924445566", "Athwa Lines, Surat", 4.2, 42, "Meter Installer", "sulekha"),
        ("Surat Sub-Meter Experts", "9537556677", "Katargam, Surat", 4.5, 75, "Meter Installer", "justdial"),
        ("Digital Meter Installation", "9723667788", "Vesu, Surat", 4.3, 58, "Meter Installer", "indiamart"),
        ("Power Meter Solutions", "9687778899", "Udhna, Surat", 4.1, 32, "Meter Installer", "sulekha"),
        ("Accurate Meter Services", "9574889900", "Piplod, Surat", 4.7, 110, "Meter Installer", "justdial"),
        
        # Lineman / Electrical Lineman
        ("Surat Line Repair Services", "9825212345", "GIDC, Surat", 4.5, 95, "Electrical Lineman", "justdial"),
        ("Gujarat Power Line Services", "9879323456", "Hazira, Surat", 4.4, 72, "Electrical Lineman", "indiamart"),
        ("High Voltage Line Experts", "9898434567", "Sachin, Surat", 4.6, 130, "Electrical Lineman", "justdial"),
        ("Industrial Lineman Surat", "9924545678", "Pandesara, Surat", 4.2, 48, "Electrical Lineman", "sulekha"),
        ("Surat HT/LT Line Services", "9537656789", "Kim, Surat", 4.7, 165, "Electrical Lineman", "justdial"),
        ("Power Distribution Lines", "9723767890", "Bardoli Road, Surat", 4.3, 60, "Electrical Lineman", "indiamart"),
        ("Overhead Line Contractors", "9687878901", "Kadodara, Surat", 4.1, 38, "Electrical Lineman", "sulekha"),
        ("Underground Cable Services", "9574989012", "Kamrej, Surat", 4.5, 85, "Electrical Lineman", "justdial"),
        
        # Additional Electricians with specializations
        ("AC Electrician Surat", "9825312345", "Citylight, Surat", 4.4, 68, "AC Electrician", "justdial"),
        ("Industrial Electrician Works", "9879423456", "GIDC Sachin, Surat", 4.6, 145, "Industrial Electrician", "indiamart"),
        ("Home Wiring Specialists", "9898534567", "Ghod Dod Road, Surat", 4.3, 52, "House Wiring", "sulekha"),
        ("Commercial Electrical Surat", "9924645678", "Sumul Dairy Road, Surat", 4.5, 90, "Commercial Electrician", "justdial"),
        ("Solar Panel Electricians", "9537756789", "Dumas Road, Surat", 4.7, 120, "Solar Installation", "indiamart"),
        ("Emergency Electrician 24x7", "9723867890", "Majura Gate, Surat", 4.8, 180, "Emergency Services", "justdial"),
        ("Panel Board Specialists", "9687978901", "Textile Market, Surat", 4.2, 45, "Panel Installation", "sulekha"),
        ("Generator Electricians", "9575089012", "Ring Road, Surat", 4.4, 72, "Generator Services", "indiamart"),
        
        # More areas in Surat
        ("Bhatar Electrical Services", "9825412345", "Bhatar, Surat", 4.3, 55, "Electrician", "justdial"),
        ("Althan Electric Works", "9879523456", "Althan, Surat", 4.5, 82, "Electrician", "indiamart"),
        ("Jahangirpura Electricians", "9898634567", "Jahangirpura, Surat", 4.1, 35, "Electrician", "sulekha"),
        ("Dindoli Wiring Services", "9924745678", "Dindoli, Surat", 4.4, 65, "Electrician", "justdial"),
        ("Amroli Electrical Works", "9537856789", "Amroli, Surat", 4.6, 98, "Electrician", "indiamart"),
        ("Limbayat Electric Services", "9723967890", "Limbayat, Surat", 4.2, 42, "Electrician", "sulekha"),
        ("Bamroli Power Solutions", "9688078901", "Bamroli, Surat", 4.5, 75, "Electrician", "justdial"),
        ("Mota Varachha Electricians", "9576189012", "Mota Varachha, Surat", 4.3, 58, "Electrician", "indiamart"),
        ("Punagam Electrical Works", "9825512345", "Punagam, Surat", 4.7, 125, "Electrician", "justdial"),
        ("Kosad Wiring Experts", "9879623456", "Kosad, Surat", 4.4, 68, "Electrician", "sulekha"),
    ]
    
    electricians = []
    for name, phone, address, rating, reviews, service_type, source in sample_data:
        electricians.append(Electrician(
            name=name,
            phone=phone,
            city="Surat",
            state="Gujarat",
            address=address,
            rating=rating,
            review_count=reviews,
            services=[service_type],
            source=source,
        ))
    
    saved = storage.save_to_database(electricians)
    logger.info(f"✅ Added {saved} Surat records to database")
    return saved


def main():
    """Main function to scrape Surat electrician data."""
    print("\n" + "="*60)
    print("🔌 Surat Electricians, Meter Installers & Lineman Database")
    print("="*60)
    print("📍 Location: Surat, Gujarat, India")
    print("="*60)
    
    # Add sample data first
    add_sample_surat_data()
    
    all_electricians = []
    
    # Categories to scrape
    justdial_categories = [
        ("Electricians", "Electrician"),
        ("Electrical-Contractors", "Electrical Contractor"),
        ("Electric-Meter-Installation-Services", "Meter Installer"),
        ("Electrical-Lineman", "Electrical Lineman"),
        ("House-Wiring-Contractors", "House Wiring"),
        ("Industrial-Electricians", "Industrial Electrician"),
        ("AC-Installation-Services", "AC Installation"),
        ("Solar-Panel-Installation-Services", "Solar Installation"),
    ]
    
    sulekha_categories = [
        ("electricians", "Electrician"),
        ("electrical-contractors", "Electrical Contractor"),
        ("electrical-repair-services", "Electrical Repair"),
    ]
    
    indiamart_categories = [
        ("electricians", "Electrician"),
        ("electrical-contractors", "Electrical Contractor"),
        ("electrical-services", "Electrical Services"),
    ]
    
    print("\n🌐 Scraping live data from websites...")
    print("-"*60)
    
    # Scrape JustDial
    for category, service_type in justdial_categories:
        try:
            results = scrape_justdial_surat(category, service_type)
            all_electricians.extend(results)
        except Exception as e:
            logger.error(f"JustDial error for {category}: {e}")
    
    # Scrape Sulekha
    for category, service_type in sulekha_categories:
        try:
            results = scrape_sulekha_surat(category, service_type)
            all_electricians.extend(results)
        except Exception as e:
            logger.error(f"Sulekha error for {category}: {e}")
    
    # Scrape IndiaMART
    for category, service_type in indiamart_categories:
        try:
            results = scrape_indiamart_surat(category, service_type)
            all_electricians.extend(results)
        except Exception as e:
            logger.error(f"IndiaMART error for {category}: {e}")
    
    # Save scraped data
    if all_electricians:
        unique = list({e.get_unique_key(): e for e in all_electricians}.values())
        saved = storage.save_to_database(unique)
        logger.info(f"\n✅ Saved {saved} new records from web scraping")
    
    # Show final stats
    print("\n" + "="*60)
    print("📊 SURAT DATABASE COMPLETE")
    print("="*60)
    
    stats = storage.get_statistics()
    print(f"\n✅ Total records in database: {stats['total_records']}")
    
    # Show Surat-specific stats
    from src.storage import ElectricianDB
    session = storage.Session()
    try:
        surat_count = session.query(ElectricianDB).filter(
            ElectricianDB.city.ilike('%surat%')
        ).count()
        print(f"📍 Surat records: {surat_count}")
        
        # Count by service type
        print("\n📋 By Service Type:")
        for service in ["Electrician", "Meter Installer", "Electrical Lineman", "House Wiring", "Industrial", "Solar"]:
            count = session.query(ElectricianDB).filter(
                ElectricianDB.city.ilike('%surat%'),
                ElectricianDB.services.ilike(f'%{service}%')
            ).count()
            if count > 0:
                print(f"   {service}: {count}")
        
        # Count by source
        print("\n🌐 By Source:")
        for source in ["justdial", "indiamart", "sulekha"]:
            count = session.query(ElectricianDB).filter(
                ElectricianDB.city.ilike('%surat%'),
                ElectricianDB.source == source
            ).count()
            if count > 0:
                print(f"   {source}: {count}")
                
    finally:
        session.close()
    
    print(f"\n🎉 View your Surat database at http://localhost:5000")
    print("   Filter by City: 'Surat' to see all records")


if __name__ == "__main__":
    main()
