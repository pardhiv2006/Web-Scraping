"""
SQLAlchemy ORM model for the businesses table.
"""
from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from database import Base


class Business(Base):
    __tablename__ = "businesses"

    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String, nullable=False)
    registration_number = Column(String, nullable=False, index=True)
    country = Column(String, nullable=False, index=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True, index=True)
    registration_date = Column(String, nullable=True)
    address = Column(String, nullable=True)
    status = Column(String, nullable=True, default="Active")
    source_url = Column(String, nullable=True)
    
    # Enriched fields
    email = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    website = Column(String, nullable=True)
    ceo_name = Column(String, nullable=True)
    ceo_email = Column(String, nullable=True)
    founder_name = Column(String, nullable=True)
    linkedin_url = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    employee_count = Column(String, nullable=True)
    revenue = Column(String, nullable=True)
    description = Column(String, nullable=True)

    scraped_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("registration_number", "country", name="uq_reg_country"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "company_name": self.company_name,
            "registration_number": self.registration_number,
            "country": self.country,
            "city": self.city,
            "state": self.state,
            "registration_date": self.registration_date,
            "address": self.address,
            "status": self.status,
            "source_url": self.source_url,
            "email": self.email,
            "phone": self.phone,
            "website": self.website,
            "ceo_name": self.ceo_name,
            "ceo_email": self.ceo_email,
            "founder_name": self.founder_name,
            "linkedin_url": self.linkedin_url,
            "industry": self.industry,
            "employee_count": self.employee_count,
            "revenue": self.revenue,
            "description": self.description,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
        }
