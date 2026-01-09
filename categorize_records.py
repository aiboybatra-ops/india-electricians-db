#!/usr/bin/env python3
"""
Script to automatically categorize existing records and assign smart meter scores.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.storage import DataStorage, ElectricianDB
import json
import re


def categorize_business(name: str, services: list = None) -> tuple:
    """
    Categorize a business based on name and services.
    Returns (category, description, smart_meter_score, notes)
    """
    name_lower = name.lower() if name else ''
    services_text = ' '.join(services or []).lower()
    combined = f"{name_lower} {services_text}"
    
    # Default values
    category = "Electrician"
    description = ""
    score = 50
    notes = ""
    
    # Meter Installation - Highest priority for smart meter work
    if any(word in combined for word in ['meter', 'metering', 'prepaid', 'energy meter', 'utility']):
        category = "Meter Installer"
        description = "Specializes in energy meter installation and maintenance"
        score = 95
        notes = "High fit - Direct experience with meter installation. Likely familiar with utility requirements and protocols."
    
    # Electrical Contractors - Good for smart meter projects
    elif any(word in combined for word in ['contractor', 'contracting', 'project', 'turnkey', 'commercial']):
        category = "Electrical Contractor"
        description = "Electrical contracting and project work"
        score = 80
        notes = "Good fit - Contractors often handle utility-scale projects and can manage smart meter rollouts."
    
    # Industrial Electricians - Strong technical background
    elif any(word in combined for word in ['industrial', 'factory', 'plant', 'manufacturing', 'hv', 'high voltage']):
        category = "Industrial Electrician"
        description = "Industrial electrical systems and high-voltage work"
        score = 75
        notes = "Good fit - Strong technical skills for complex installations. May need training on specific meter models."
    
    # Electrical Engineers / Consultants
    elif any(word in combined for word in ['engineer', 'consultant', 'solution', 'technical']):
        category = "Electrical Contractor"
        description = "Electrical engineering and consulting services"
        score = 70
        notes = "Moderate fit - Technical knowledge is strong but may focus more on design than installation."
    
    # AC/HVAC Electricians
    elif any(word in combined for word in ['ac ', 'air condition', 'hvac', 'cooling', 'refrigeration']):
        category = "AC Electrician"
        description = "Air conditioning and HVAC electrical work"
        score = 45
        notes = "Lower fit - Specialized in HVAC systems. Would need training for meter installation."
    
    # Solar/Renewable
    elif any(word in combined for word in ['solar', 'renewable', 'panel', 'photovoltaic', 'inverter']):
        category = "Solar Installation"
        description = "Solar panel and renewable energy installation"
        score = 70
        notes = "Good fit - Experience with grid connections and metering for net metering. Familiar with utility requirements."
    
    # House Wiring / Residential
    elif any(word in combined for word in ['house', 'home', 'residential', 'domestic', 'wiring', 'rewiring']):
        category = "House Wiring"
        description = "Residential wiring and electrical repairs"
        score = 55
        notes = "Moderate fit - Basic electrical skills but may need training on smart meter protocols."
    
    # Lineman / Utility Workers
    elif any(word in combined for word in ['lineman', 'linemen', 'line work', 'utility', 'power line']):
        category = "Electrical Lineman"
        description = "Power line installation and maintenance"
        score = 85
        notes = "High fit - Experienced with utility infrastructure. Ideal for meter installation at service points."
    
    # General Electricians
    else:
        category = "Electrician"
        description = "General electrical services and repairs"
        score = 50
        notes = "Moderate fit - General electrical experience. Training on smart meters would be required."
    
    # Boost score based on positive indicators
    if any(word in combined for word in ['certified', 'licensed', 'approved', 'authorized']):
        score = min(score + 10, 100)
        notes += " Certified/licensed professional."
    
    if any(word in combined for word in ['government', 'govt', 'discom', 'utility approved']):
        score = min(score + 15, 100)
        notes += " Has government/utility experience."
    
    # Reduce score for potential issues
    if any(word in combined for word in ['repair only', 'fan', 'light', 'appliance']):
        score = max(score - 15, 10)
        notes += " May focus on small repairs."
    
    return category, description, score, notes


def update_records():
    """Update all records with categories and smart meter scores."""
    storage = DataStorage()
    session = storage.Session()
    
    try:
        # Get all records
        records = session.query(ElectricianDB).all()
        print(f"Processing {len(records)} records...")
        
        updated = 0
        for record in records:
            # Parse services
            services = []
            if record.services:
                try:
                    services = json.loads(record.services)
                except:
                    services = [record.services]
            
            # Get categorization
            category, description, score, notes = categorize_business(
                record.name, 
                services
            )
            
            # Update record
            record.category = category
            record.service_description = description
            record.smart_meter_score = score
            record.smart_meter_notes = notes
            
            updated += 1
            if updated % 50 == 0:
                print(f"  Processed {updated} records...")
        
        # Commit changes
        session.commit()
        print(f"\n✓ Updated {updated} records with categories and smart meter scores")
        
        # Print summary
        print("\nCategory Distribution:")
        categories = session.query(
            ElectricianDB.category,
            session.query(ElectricianDB).filter(ElectricianDB.category == ElectricianDB.category).count()
        ).group_by(ElectricianDB.category).all()
        
        from sqlalchemy import func
        cat_counts = session.query(
            ElectricianDB.category,
            func.count(ElectricianDB.id)
        ).group_by(ElectricianDB.category).all()
        
        for cat, count in cat_counts:
            print(f"  {cat}: {count}")
        
        print("\nSmart Meter Score Distribution:")
        high_fit = session.query(ElectricianDB).filter(ElectricianDB.smart_meter_score >= 70).count()
        medium_fit = session.query(ElectricianDB).filter(
            ElectricianDB.smart_meter_score >= 40,
            ElectricianDB.smart_meter_score < 70
        ).count()
        low_fit = session.query(ElectricianDB).filter(ElectricianDB.smart_meter_score < 40).count()
        
        print(f"  High Fit (>=70): {high_fit}")
        print(f"  Medium Fit (40-69): {medium_fit}")
        print(f"  Low Fit (<40): {low_fit}")
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    print("=" * 60)
    print("Electricians Database - Categorization & Smart Meter Scoring")
    print("=" * 60)
    print()
    update_records()
