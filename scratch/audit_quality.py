import os
import sys
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.join(os.getcwd(), "backend"))
from database import DATABASE_URL
from models.business import Business

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

AGGREGATOR_KEYWORDS = [
    "opencorporates.com", "bizapedia.com", "yelp.com", "yellowpages.com", 
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com",
    "bloomberg.com", "crunchbase.com", "zoominfo.com", "dnb.com", 
    "manta.com", "bbb.org", "indeed.com", "glassdoor.com", "wikipedia.org",
    "mapquest.com", "chamberofcommerce.com", "find-and-update.company-information.service.gov.uk"
]

def audit_quality():
    db = SessionLocal()
    try:
        # 1. Check "Not Available" counts
        not_available_email = db.query(Business).filter(Business.email == "Not Available").count()
        not_available_phone = db.query(Business).filter(Business.phone == "Not Available").count()
        
        print(f"Records with 'Not Available' Email: {not_available_email}")
        print(f"Records with 'Not Available' Phone: {not_available_phone}")
        
        # 2. Check for Aggregator Links in Website
        aggregator_links = 0
        for keyword in AGGREGATOR_KEYWORDS:
            count = db.query(Business).filter(Business.website.contains(keyword)).count()
            if count > 0:
                print(f"  [Aggregator] {keyword}: {count} records")
                aggregator_links += count
        
        print(f"Total records with Aggregator links as Website: {aggregator_links}")
        
        # 3. Check for broken links (generic placeholders)
        placeholders = db.query(Business).filter(or_(
            Business.website == "Not Available",
            Business.website == "",
            Business.website == "-",
            Business.website.contains("google.com/search")
        )).count()
        print(f"Records with missing/placeholder Website: {placeholders}")

    finally:
        db.close()

if __name__ == "__main__":
    audit_quality()
