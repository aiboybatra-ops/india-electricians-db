"""
Data models for storing scraped electrician information.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional, List
from datetime import datetime


@dataclass
class Electrician:
    """Data model for an electrician/lineman."""
    
    # Basic Information
    name: str
    phone: str
    
    # Location
    city: str
    state: str
    address: Optional[str] = None
    pincode: Optional[str] = None
    
    # Business Details
    business_name: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    
    # Service Information
    services: List[str] = field(default_factory=list)
    experience_years: Optional[int] = None
    
    # Ratings and Reviews
    rating: Optional[float] = None
    review_count: Optional[int] = None
    
    # Source Information
    source: str = ""  # e.g., "justdial", "indiamart", "sulekha", "google"
    source_url: Optional[str] = None
    
    # Metadata
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    verified: bool = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "Electrician":
        """Create from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
    
    def get_unique_key(self) -> str:
        """Generate a unique key for deduplication."""
        # Use phone number as primary key, normalize it
        phone_normalized = "".join(filter(str.isdigit, self.phone))[-10:]
        return f"{phone_normalized}_{self.city.lower()}_{self.state.lower()}"
    
    def __hash__(self):
        return hash(self.get_unique_key())
    
    def __eq__(self, other):
        if isinstance(other, Electrician):
            return self.get_unique_key() == other.get_unique_key()
        return False


@dataclass
class ScrapeResult:
    """Result of a scraping operation."""
    
    success: bool
    source: str
    city: str
    state: str
    electricians: List[Electrician] = field(default_factory=list)
    error_message: Optional[str] = None
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    pages_scraped: int = 0
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "source": self.source,
            "city": self.city,
            "state": self.state,
            "electricians_count": len(self.electricians),
            "error_message": self.error_message,
            "scraped_at": self.scraped_at,
            "pages_scraped": self.pages_scraped,
        }
