"""
Data storage module for saving scraped electrician data.
Supports CSV, JSON, and SQLite storage.
"""
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from src.models import Electrician, ScrapeResult
from src.config import OUTPUT_DIR, DATABASE_URL

Base = declarative_base()


class ElectricianDB(Base):
    """SQLAlchemy model for electrician records."""
    
    __tablename__ = "electricians"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    phone = Column(String(20), nullable=False, index=True)
    city = Column(String(100), nullable=False, index=True)
    state = Column(String(100), nullable=False, index=True)
    address = Column(Text)
    pincode = Column(String(10))
    business_name = Column(String(255))
    email = Column(String(255))
    website = Column(String(500))
    services = Column(Text)  # JSON string
    experience_years = Column(Integer)
    rating = Column(Float)
    review_count = Column(Integer)
    source = Column(String(50), nullable=False, index=True)
    source_url = Column(Text)
    verified = Column(Boolean, default=False)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    unique_key = Column(String(100), unique=True, index=True)
    
    # New fields for categorization
    category = Column(String(100), index=True)  # Electrician, Meter Installer, Lineman, etc.
    service_description = Column(Text)  # Detailed description of services
    smart_meter_score = Column(Integer, default=0)  # 0-100 score for smart meter installation fit
    smart_meter_notes = Column(Text)  # Notes on why they are/aren't a good fit
    verified_by = Column(String(100))  # Who verified this record
    verified_at = Column(DateTime)  # When was it verified
    
    @classmethod
    def from_electrician(cls, electrician: Electrician) -> "ElectricianDB":
        """Create from Electrician dataclass."""
        return cls(
            name=electrician.name,
            phone=electrician.phone,
            city=electrician.city,
            state=electrician.state,
            address=electrician.address,
            pincode=electrician.pincode,
            business_name=electrician.business_name,
            email=electrician.email,
            website=electrician.website,
            services=json.dumps(electrician.services) if electrician.services else None,
            experience_years=electrician.experience_years,
            rating=electrician.rating,
            review_count=electrician.review_count,
            source=electrician.source,
            source_url=electrician.source_url,
            verified=electrician.verified,
            scraped_at=datetime.fromisoformat(electrician.scraped_at) if electrician.scraped_at else datetime.utcnow(),
            unique_key=electrician.get_unique_key(),
        )
    
    def to_electrician(self) -> Electrician:
        """Convert to Electrician dataclass."""
        return Electrician(
            name=self.name,
            phone=self.phone,
            city=self.city,
            state=self.state,
            address=self.address,
            pincode=self.pincode,
            business_name=self.business_name,
            email=self.email,
            website=self.website,
            services=json.loads(self.services) if self.services else [],
            experience_years=self.experience_years,
            rating=self.rating,
            review_count=self.review_count,
            source=self.source,
            source_url=self.source_url,
            verified=self.verified,
            scraped_at=self.scraped_at.isoformat() if self.scraped_at else None,
        )


class DataStorage:
    """Class for storing scraped data in various formats."""
    
    def __init__(self, output_dir: Path = None, database_url: str = None):
        self.output_dir = output_dir or OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up SQLite database
        self.database_url = database_url or DATABASE_URL
        self.engine = create_engine(self.database_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def save_to_csv(
        self,
        electricians: List[Electrician],
        filename: str = None,
        append: bool = True,
    ) -> str:
        """Save electricians to CSV file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"electricians_{timestamp}.csv"
        
        filepath = self.output_dir / filename
        file_exists = filepath.exists()
        
        mode = "a" if append and file_exists else "w"
        
        fieldnames = [
            "name", "phone", "city", "state", "address", "pincode",
            "business_name", "email", "website", "services",
            "experience_years", "rating", "review_count",
            "source", "source_url", "verified", "scraped_at"
        ]
        
        with open(filepath, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if mode == "w" or not file_exists:
                writer.writeheader()
            
            for electrician in electricians:
                row = electrician.to_dict()
                # Convert services list to string
                if row.get("services"):
                    row["services"] = "; ".join(row["services"])
                writer.writerow(row)
        
        return str(filepath)
    
    def save_to_json(
        self,
        electricians: List[Electrician],
        filename: str = None,
        append: bool = True,
    ) -> str:
        """Save electricians to JSON file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"electricians_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        existing_data = []
        if append and filepath.exists():
            with open(filepath, "r", encoding="utf-8") as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
        
        new_data = [e.to_dict() for e in electricians]
        
        # Deduplicate
        all_data = existing_data + new_data
        unique_data = list({d["phone"]: d for d in all_data}.values())
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(unique_data, f, indent=2, ensure_ascii=False)
        
        return str(filepath)
    
    def save_to_database(
        self,
        electricians: List[Electrician],
        update_existing: bool = True,
    ) -> int:
        """Save electricians to SQLite database with deduplication."""
        session = self.Session()
        saved_count = 0
        
        try:
            for electrician in electricians:
                unique_key = electrician.get_unique_key()
                
                # Check if record exists
                existing = session.query(ElectricianDB).filter_by(unique_key=unique_key).first()
                
                if existing:
                    if update_existing:
                        # Update existing record with new data
                        db_record = ElectricianDB.from_electrician(electrician)
                        for key in ["name", "address", "rating", "review_count", "source_url"]:
                            if getattr(db_record, key):
                                setattr(existing, key, getattr(db_record, key))
                        existing.scraped_at = datetime.utcnow()
                else:
                    # Insert new record
                    db_record = ElectricianDB.from_electrician(electrician)
                    session.add(db_record)
                    saved_count += 1
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
        
        return saved_count
    
    def load_from_database(
        self,
        city: str = None,
        state: str = None,
        source: str = None,
        limit: int = None,
    ) -> List[Electrician]:
        """Load electricians from database with optional filters."""
        session = self.Session()
        
        try:
            query = session.query(ElectricianDB)
            
            if city:
                query = query.filter(ElectricianDB.city.ilike(f"%{city}%"))
            if state:
                query = query.filter(ElectricianDB.state.ilike(f"%{state}%"))
            if source:
                query = query.filter(ElectricianDB.source == source)
            if limit:
                query = query.limit(limit)
            
            records = query.all()
            return [r.to_electrician() for r in records]
            
        finally:
            session.close()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about stored data."""
        session = self.Session()
        
        try:
            total = session.query(ElectricianDB).count()
            
            by_state = {}
            from sqlalchemy import func
            state_counts = session.query(
                ElectricianDB.state,
                func.count(ElectricianDB.id)
            ).group_by(ElectricianDB.state).all()
            by_state = {state: count for state, count in state_counts}
            
            by_source = {}
            source_counts = session.query(
                ElectricianDB.source,
                func.count(ElectricianDB.id)
            ).group_by(ElectricianDB.source).all()
            by_source = {source: count for source, count in source_counts}
            
            by_city = {}
            city_counts = session.query(
                ElectricianDB.city,
                func.count(ElectricianDB.id)
            ).group_by(ElectricianDB.city).order_by(func.count(ElectricianDB.id).desc()).limit(20).all()
            by_city = {city: count for city, count in city_counts}
            
            return {
                "total_records": total,
                "by_state": by_state,
                "by_source": by_source,
                "top_cities": by_city,
            }
            
        finally:
            session.close()
    
    def export_to_excel(self, filename: str = None) -> str:
        """Export all data to Excel file."""
        import pandas as pd
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"electricians_{timestamp}.xlsx"
        
        filepath = self.output_dir / filename
        
        electricians = self.load_from_database()
        df = pd.DataFrame([e.to_dict() for e in electricians])
        
        # Convert services list to string
        if "services" in df.columns:
            df["services"] = df["services"].apply(
                lambda x: "; ".join(x) if isinstance(x, list) else x
            )
        
        df.to_excel(filepath, index=False, engine="openpyxl")
        
        return str(filepath)
    
    def deduplicate_database(self) -> int:
        """Remove duplicate entries from database."""
        session = self.Session()
        removed = 0
        
        try:
            # Find duplicates based on phone number
            from sqlalchemy import func
            
            duplicates = session.query(
                ElectricianDB.phone,
                func.count(ElectricianDB.id).label("count")
            ).group_by(ElectricianDB.phone).having(func.count(ElectricianDB.id) > 1).all()
            
            for phone, count in duplicates:
                records = session.query(ElectricianDB).filter_by(phone=phone).all()
                # Keep the one with highest rating or most recent
                records.sort(key=lambda x: (x.rating or 0, x.scraped_at or datetime.min), reverse=True)
                
                for record in records[1:]:  # Remove all but the first
                    session.delete(record)
                    removed += 1
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
        
        return removed
