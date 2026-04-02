import os
import sys
import logging
from sqlalchemy.orm import Session
from sqlalchemy import func

# Add backend to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)

from database import SessionLocal
from models.business import Business

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Validator")

def validate_and_cleanup():
    db = SessionLocal()
    try:
        # 1. Check for duplicates in Email, Phone, Website, LinkedIn, Address
        for field in ["email", "phone", "website", "linkedin_url", "address"]:
            # Find duplicates
            subquery = db.query(getattr(Business, field), func.count(Business.id)).filter(getattr(Business, field) != None, getattr(Business, field) != "").group_by(getattr(Business, field)).having(func.count(Business.id) > 1).all()
            if subquery:
                logger.warning(f"Found {len(subquery)} duplicate values for field: {field}")
                # We won't delete, but we should log them for manual review
            else:
                logger.info(f"No duplicates found for {field}.")

        # 2. Check for missing mandatory fields
        mandatory = ["company_name", "address", "city", "state", "country", "email", "phone", "website", "linkedin_url", "industry", "description"]
        for field in mandatory:
            count = db.query(Business).filter((getattr(Business, field) == None) | (getattr(Business, field) == "")).count()
            if count > 0:
                logger.warning(f"Field {field} still has {count} empty values.")
            else:
                logger.info(f"Field {field} is 100% complete.")

        # 3. Basic URL validation
        for field in ["website", "linkedin_url"]:
            invalid = db.query(Business).filter(getattr(Business, field).notlike("http%")).filter(getattr(Business, field) != None, getattr(Business, field) != "").all()
            if invalid:
                logger.warning(f"Found {len(invalid)} invalid URLs in {field}. Fixing prefixes...")
                for biz in invalid:
                    val = getattr(biz, field)
                    if val and not val.startswith("http"):
                        setattr(biz, field, "https://" + val.strip("/"))
                db.commit()

        # 4. Email consistency with website domain
        # (This is a soft requirement, but good for quality)
        logger.info("Validation complete.")
    finally:
        db.close()

if __name__ == "__main__":
    validate_and_cleanup()
