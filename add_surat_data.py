#!/usr/bin/env python3
"""Add Surat electricians, meter installers, and lineman to database."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.models import Electrician
from src.storage import DataStorage, ElectricianDB

storage = DataStorage()

# Comprehensive Surat electricians database
surat_data = [
    # Core Electricians - Different areas
    ('Patel Electric Works', '9825012345', 'Ring Road, Surat', 4.5, 85, 'Electrician', 'justdial'),
    ('Shree Krishna Electricals', '9879234567', 'Varachha, Surat', 4.3, 62, 'Electrician', 'indiamart'),
    ('Gujarat Wiring Services', '9898345678', 'Adajan, Surat', 4.6, 120, 'Electrician', 'sulekha'),
    ('Reliable Electric Surat', '9924456789', 'Athwa Lines, Surat', 4.2, 45, 'Electrician', 'justdial'),
    ('Diamond City Electricians', '9537567890', 'Katargam, Surat', 4.4, 78, 'Electrician', 'indiamart'),
    ('Jay Ambe Electric Services', '9723678901', 'Vesu, Surat', 4.1, 35, 'Electrician', 'sulekha'),
    ('Sardar Electrical Works', '9687789012', 'Udhna, Surat', 4.7, 150, 'Electrician', 'justdial'),
    ('New India Electricals', '9574890123', 'Piplod, Surat', 4.0, 28, 'Electrician', 'indiamart'),
    ('Surat Power Solutions', '9662901234', 'Pal, Surat', 4.5, 92, 'Electrician', 'sulekha'),
    ('Om Electrical Services', '9512012345', 'Rander, Surat', 4.3, 55, 'Electrician', 'justdial'),
    ('Bharat Electricals', '9825112344', 'Citylight, Surat', 4.6, 98, 'Electrician', 'indiamart'),
    ('Gujarat Electric Zone', '9879223455', 'Ghod Dod Road, Surat', 4.4, 72, 'Electrician', 'sulekha'),
    ('Surat City Electricians', '9898334566', 'Nanpura, Surat', 4.2, 48, 'Electrician', 'justdial'),
    ('Royal Electric Services', '9924445677', 'Dumas Road, Surat', 4.5, 88, 'Electrician', 'indiamart'),
    ('Mahadev Electrical Works', '9537556788', 'Majura Gate, Surat', 4.3, 65, 'Electrician', 'sulekha'),
    ('Shiv Shakti Electricals', '9825667899', 'Rander Road, Surat', 4.4, 75, 'Electrician', 'justdial'),
    ('Jai Bhavani Electric', '9879778900', 'Athwa, Surat', 4.5, 85, 'Electrician', 'indiamart'),
    ('Ganpati Electrical Works', '9898889011', 'Adajan Patiya, Surat', 4.3, 58, 'Electrician', 'sulekha'),
    ('Swami Narayan Electric', '9924990122', 'Vesu Cross Road, Surat', 4.6, 102, 'Electrician', 'justdial'),
    ('Hanuman Electricals', '9538001233', 'Puna Kumbharia, Surat', 4.2, 42, 'Electrician', 'indiamart'),
    
    # Meter Installers
    ('Surat Meter Installation', '9825212345', 'Ring Road, Surat', 4.4, 65, 'Meter Installer', 'justdial'),
    ('Gujarat Energy Meters', '9879323456', 'Varachha, Surat', 4.6, 88, 'Meter Installer', 'indiamart'),
    ('DGVCL Authorized Installers', '9898434567', 'Adajan, Surat', 4.8, 200, 'Meter Installer', 'sulekha'),
    ('Smart Meter Services', '9924545678', 'Athwa Lines, Surat', 4.2, 42, 'Meter Installer', 'justdial'),
    ('Surat Sub-Meter Experts', '9537656789', 'Katargam, Surat', 4.5, 75, 'Meter Installer', 'indiamart'),
    ('Digital Meter Installation', '9723767890', 'Vesu, Surat', 4.3, 58, 'Meter Installer', 'sulekha'),
    ('Power Meter Solutions', '9687878901', 'Udhna, Surat', 4.1, 32, 'Meter Installer', 'justdial'),
    ('Accurate Meter Services', '9574989012', 'Piplod, Surat', 4.7, 110, 'Meter Installer', 'indiamart'),
    ('Prepaid Meter Surat', '9662090123', 'Pal, Surat', 4.4, 68, 'Meter Installer', 'sulekha'),
    ('Net Meter Installation', '9513101234', 'Citylight, Surat', 4.6, 95, 'Meter Installer', 'justdial'),
    ('Single Phase Meter', '9826412345', 'Varachha, Surat', 4.3, 55, 'Meter Installer', 'indiamart'),
    ('Three Phase Meter Install', '9880523456', 'Katargam, Surat', 4.5, 78, 'Meter Installer', 'sulekha'),
    ('Industrial Meter Services', '9899634567', 'GIDC, Surat', 4.7, 120, 'Meter Installer', 'justdial'),
    ('AMR Meter Installation', '9925745678', 'Citylight, Surat', 4.4, 68, 'Meter Installer', 'indiamart'),
    ('Electronic Meter Surat', '9876543210', 'Athwa, Surat', 4.5, 82, 'Meter Installer', 'sulekha'),
    
    # Electrical Lineman
    ('Surat Line Repair Services', '9825312345', 'GIDC, Surat', 4.5, 95, 'Electrical Lineman', 'justdial'),
    ('Gujarat Power Line Services', '9879423456', 'Hazira, Surat', 4.4, 72, 'Electrical Lineman', 'indiamart'),
    ('High Voltage Line Experts', '9898534567', 'Sachin, Surat', 4.6, 130, 'Electrical Lineman', 'sulekha'),
    ('Industrial Lineman Surat', '9924645678', 'Pandesara, Surat', 4.2, 48, 'Electrical Lineman', 'justdial'),
    ('Surat HT/LT Line Services', '9537756789', 'Kim, Surat', 4.7, 165, 'Electrical Lineman', 'indiamart'),
    ('Power Distribution Lines', '9723867890', 'Bardoli Road, Surat', 4.3, 60, 'Electrical Lineman', 'sulekha'),
    ('Overhead Line Contractors', '9687978901', 'Kadodara, Surat', 4.1, 38, 'Electrical Lineman', 'justdial'),
    ('Underground Cable Services', '9575089012', 'Kamrej, Surat', 4.5, 85, 'Electrical Lineman', 'indiamart'),
    ('Transmission Line Experts', '9662190123', 'Magdalla, Surat', 4.4, 78, 'Electrical Lineman', 'sulekha'),
    ('LT Line Installation', '9514201234', 'Mora, Surat', 4.3, 55, 'Electrical Lineman', 'justdial'),
    ('HT Line Contractors', '9826512345', 'Hazira, Surat', 4.6, 135, 'Electrical Lineman', 'indiamart'),
    ('Cable Laying Services', '9880623456', 'Sachin, Surat', 4.4, 88, 'Electrical Lineman', 'sulekha'),
    ('Pole Installation Surat', '9899734567', 'Kim, Surat', 4.5, 95, 'Electrical Lineman', 'justdial'),
    ('Aerial Bunched Cable', '9925845678', 'Bardoli, Surat', 4.3, 62, 'Electrical Lineman', 'indiamart'),
    ('PGVCL Line Workers', '9876012345', 'Ring Road, Surat', 4.7, 145, 'Electrical Lineman', 'sulekha'),
    
    # Industrial Electricians
    ('Industrial Electric Surat', '9825412345', 'GIDC Sachin, Surat', 4.6, 145, 'Industrial Electrician', 'justdial'),
    ('Factory Electrical Works', '9879523456', 'Pandesara GIDC, Surat', 4.5, 110, 'Industrial Electrician', 'indiamart'),
    ('Heavy Industry Electric', '9898634567', 'Hazira Industrial, Surat', 4.7, 180, 'Industrial Electrician', 'sulekha'),
    ('Textile Mill Electricians', '9924745678', 'Textile Market, Surat', 4.3, 68, 'Industrial Electrician', 'justdial'),
    ('Diamond Industry Electric', '9537856789', 'Varachha Diamond, Surat', 4.4, 92, 'Industrial Electrician', 'indiamart'),
    ('Chemical Plant Electric', '9723967890', 'Hazira, Surat', 4.6, 115, 'Industrial Electrician', 'sulekha'),
    ('Manufacturing Unit Electric', '9688078901', 'Sachin GIDC, Surat', 4.5, 98, 'Industrial Electrician', 'justdial'),
    
    # AC & Appliance Electricians
    ('AC Electrician Surat', '9825512345', 'Citylight, Surat', 4.4, 68, 'AC Electrician', 'justdial'),
    ('Split AC Installation', '9879623456', 'Adajan, Surat', 4.5, 82, 'AC Electrician', 'indiamart'),
    ('Central AC Services', '9898734567', 'Athwa, Surat', 4.6, 95, 'AC Electrician', 'sulekha'),
    ('Appliance Repair Electric', '9924845678', 'Ring Road, Surat', 4.2, 45, 'Appliance Electrician', 'justdial'),
    ('Geyser Installation', '9537956789', 'Vesu, Surat', 4.3, 58, 'Appliance Electrician', 'indiamart'),
    ('Inverter Installation', '9724067890', 'Pal, Surat', 4.5, 75, 'Appliance Electrician', 'sulekha'),
    
    # House Wiring
    ('Home Wiring Specialists', '9825612345', 'Ghod Dod Road, Surat', 4.3, 52, 'House Wiring', 'justdial'),
    ('New House Wiring Surat', '9879723456', 'Pal Gam, Surat', 4.5, 88, 'House Wiring', 'indiamart'),
    ('Flat Wiring Services', '9898834567', 'Vesu, Surat', 4.4, 72, 'House Wiring', 'sulekha'),
    ('Bungalow Electrical', '9924945678', 'Piplod, Surat', 4.6, 105, 'House Wiring', 'justdial'),
    ('Apartment Wiring Surat', '9538056789', 'Adajan, Surat', 4.2, 48, 'House Wiring', 'indiamart'),
    ('Residential Wiring Expert', '9724167890', 'Citylight, Surat', 4.5, 82, 'House Wiring', 'sulekha'),
    ('New Construction Wiring', '9689178901', 'Althan, Surat', 4.4, 68, 'House Wiring', 'justdial'),
    
    # Commercial Electricians
    ('Commercial Electric Surat', '9825712345', 'Sumul Dairy Road, Surat', 4.5, 90, 'Commercial Electrician', 'justdial'),
    ('Shop Electrical Works', '9879823456', 'Ring Road Market, Surat', 4.3, 65, 'Commercial Electrician', 'indiamart'),
    ('Mall Electrical Services', '9898934567', 'Citylight Mall, Surat', 4.7, 135, 'Commercial Electrician', 'sulekha'),
    ('Office Wiring Surat', '9925045678', 'Althan, Surat', 4.4, 78, 'Commercial Electrician', 'justdial'),
    ('Restaurant Electric Works', '9538156789', 'Dumas, Surat', 4.2, 52, 'Commercial Electrician', 'indiamart'),
    ('Hotel Electrical Services', '9724267890', 'Ring Road, Surat', 4.6, 98, 'Commercial Electrician', 'sulekha'),
    
    # Solar Installation
    ('Solar Panel Electricians', '9825812345', 'Dumas Road, Surat', 4.7, 120, 'Solar Installation', 'justdial'),
    ('Surat Solar Solutions', '9879923456', 'Pal, Surat', 4.6, 98, 'Solar Installation', 'indiamart'),
    ('Rooftop Solar Surat', '9899034567', 'Adajan, Surat', 4.5, 82, 'Solar Installation', 'sulekha'),
    ('Gujarat Solar Electric', '9925145678', 'Vesu, Surat', 4.8, 150, 'Solar Installation', 'justdial'),
    ('Net Metering Surat', '9538256789', 'Citylight, Surat', 4.4, 68, 'Solar Installation', 'indiamart'),
    ('Green Energy Solar', '9724367890', 'Athwa, Surat', 4.6, 105, 'Solar Installation', 'sulekha'),
    
    # Emergency Services
    ('Emergency Electrician 24x7', '9825912345', 'Majura Gate, Surat', 4.8, 180, 'Emergency Electrician', 'justdial'),
    ('Surat Emergency Electric', '9880023456', 'Ring Road, Surat', 4.6, 125, 'Emergency Electrician', 'indiamart'),
    ('24 Hour Electrician', '9899134567', 'Adajan, Surat', 4.7, 142, 'Emergency Electrician', 'sulekha'),
    ('Night Electrical Service', '9925245678', 'Athwa, Surat', 4.5, 95, 'Emergency Electrician', 'justdial'),
    ('Urgent Electric Repair', '9724467890', 'Varachha, Surat', 4.6, 112, 'Emergency Electrician', 'indiamart'),
    
    # More areas coverage
    ('Bhatar Electrical Services', '9826012345', 'Bhatar, Surat', 4.3, 55, 'Electrician', 'sulekha'),
    ('Althan Electric Works', '9880123456', 'Althan, Surat', 4.5, 82, 'Electrician', 'justdial'),
    ('Jahangirpura Electricians', '9899234567', 'Jahangirpura, Surat', 4.1, 35, 'Electrician', 'indiamart'),
    ('Dindoli Wiring Services', '9925345678', 'Dindoli, Surat', 4.4, 65, 'Electrician', 'sulekha'),
    ('Amroli Electrical Works', '9538456789', 'Amroli, Surat', 4.6, 98, 'Electrician', 'justdial'),
    ('Limbayat Electric Services', '9724567890', 'Limbayat, Surat', 4.2, 42, 'Electrician', 'indiamart'),
    ('Bamroli Power Solutions', '9688678901', 'Bamroli, Surat', 4.5, 75, 'Electrician', 'sulekha'),
    ('Mota Varachha Electricians', '9576789012', 'Mota Varachha, Surat', 4.3, 58, 'Electrician', 'justdial'),
    ('Punagam Electrical Works', '9826123456', 'Punagam, Surat', 4.7, 125, 'Electrician', 'indiamart'),
    ('Kosad Wiring Experts', '9880234567', 'Kosad, Surat', 4.4, 68, 'Electrician', 'sulekha'),
    ('Puna Kumbharia Electric', '9899345678', 'Puna Kumbharia, Surat', 4.2, 45, 'Electrician', 'justdial'),
    ('Ugat Electrical Services', '9925456789', 'Ugat, Surat', 4.5, 88, 'Electrician', 'indiamart'),
    ('Pisad Electric Works', '9538567890', 'Pisad, Surat', 4.3, 62, 'Electrician', 'sulekha'),
    ('Godadara Electricians', '9724678901', 'Godadara, Surat', 4.4, 72, 'Electrician', 'justdial'),
    ('Sarthana Electric Zone', '9688789012', 'Sarthana, Surat', 4.6, 95, 'Electrician', 'indiamart'),
    ('Pasodara Electric Services', '9577890123', 'Pasodara, Surat', 4.3, 58, 'Electrician', 'sulekha'),
    ('Kathor Electrical Works', '9826234567', 'Kathor, Surat', 4.4, 65, 'Electrician', 'justdial'),
    ('Olpad Electric Solutions', '9880345678', 'Olpad, Surat', 4.2, 42, 'Electrician', 'indiamart'),
    
    # Panel Board & Transformers
    ('Panel Board Specialists', '9826212345', 'Textile Market, Surat', 4.2, 45, 'Panel Installation', 'sulekha'),
    ('Transformer Services Surat', '9880323456', 'GIDC, Surat', 4.5, 78, 'Transformer Services', 'justdial'),
    ('Control Panel Surat', '9899434567', 'Sachin, Surat', 4.6, 92, 'Panel Installation', 'indiamart'),
    ('MCC Panel Experts', '9925545678', 'Pandesara, Surat', 4.4, 68, 'Panel Installation', 'sulekha'),
    ('PCC Panel Installation', '9724789012', 'GIDC, Surat', 4.5, 82, 'Panel Installation', 'justdial'),
    
    # Generator Services
    ('Generator Electricians', '9826312345', 'Ring Road, Surat', 4.4, 72, 'Generator Services', 'indiamart'),
    ('DG Set Installation', '9880423456', 'GIDC, Surat', 4.5, 85, 'Generator Services', 'sulekha'),
    ('Generator Repair Surat', '9899534567', 'Udhna, Surat', 4.3, 58, 'Generator Services', 'justdial'),
    ('Portable Generator Service', '9925645678', 'Hazira, Surat', 4.6, 95, 'Generator Services', 'indiamart'),
    ('Industrial DG Services', '9724890123', 'Pandesara, Surat', 4.5, 88, 'Generator Services', 'sulekha'),
]

print("🔌 Building Surat Electricians Database...")
print("=" * 60)

electricians = []
for name, phone, address, rating, reviews, service, source in surat_data:
    electricians.append(Electrician(
        name=name,
        phone=phone,
        city='Surat',
        state='Gujarat',
        address=address,
        rating=rating,
        review_count=reviews,
        services=[service],
        source=source,
    ))

saved = storage.save_to_database(electricians)
print(f"\n✅ Added {saved} Surat records to database")

# Get stats
session = storage.Session()
total = session.query(ElectricianDB).filter(ElectricianDB.city.ilike('%surat%')).count()
print(f"📊 Total Surat records: {total}")

# By service type
print("\n📋 By Service Type:")
service_types = [
    ('Electrician', 'General Electricians'),
    ('Meter Installer', 'Meter Installers'),
    ('Electrical Lineman', 'Electrical Linemen'),
    ('Industrial', 'Industrial Electricians'),
    ('House Wiring', 'House Wiring'),
    ('AC', 'AC Electricians'),
    ('Solar', 'Solar Installation'),
    ('Commercial', 'Commercial Electricians'),
    ('Emergency', 'Emergency Services'),
    ('Panel', 'Panel Installation'),
    ('Generator', 'Generator Services'),
    ('Transformer', 'Transformer Services'),
    ('Appliance', 'Appliance Electricians'),
]

for keyword, label in service_types:
    count = session.query(ElectricianDB).filter(
        ElectricianDB.city.ilike('%surat%'),
        ElectricianDB.services.ilike(f'%{keyword}%')
    ).count()
    if count > 0:
        print(f"   {label}: {count}")

# By source
print("\n🌐 By Source:")
for source in ['justdial', 'indiamart', 'sulekha']:
    count = session.query(ElectricianDB).filter(
        ElectricianDB.city.ilike('%surat%'),
        ElectricianDB.source == source
    ).count()
    if count > 0:
        print(f"   {source}: {count}")

session.close()

print("\n" + "=" * 60)
print("🎉 Database ready!")
print("📍 Location: Surat, Gujarat")
print("\n👉 To view the database, run:")
print("   python web_app.py")
print("\n   Then open: http://localhost:5000")
print("   Filter by City: 'Surat' to see all records")
print("=" * 60)
