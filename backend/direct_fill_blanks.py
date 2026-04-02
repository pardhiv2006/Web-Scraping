
import os
import sys
import logging
import urllib.parse
from sqlalchemy.orm import Session

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal
from models.business import Business
from services.smart_scraper import discover_company_info, smart_extract

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def fill(biz: Business):
    """
    Attempts to enrich a single business record using ONLY genuine data sources.
    No synthetic websites or fake phone numbers are injected.
    """
    country = (biz.country or "").upper()
    name = biz.company_name or "Business"

    info = discover_company_info(name, biz.state or "", country)
    website = info.get("website")

    extracted_data = {}
    if website:
        # We only do smart extraction if a real website was discovered.
        extracted_data = smart_extract(website, company_name=name)

    # Merge found data
    final_data = {**info, **extracted_data}

    # Apply ONLY what was actually found. No smart defaults/fakes.
    fields_to_update = ["email", "phone", "website", "ceo_name", "linkedin_url", "description", "industry"]
    updated_any = False
    for field in fields_to_update:
        val = final_data.get(field)
        if val:
            setattr(biz, field, val)
            updated_any = True

    return updated_any

def main():
    logger.info("Starting genuine data enrichment for blank fields...")
    db: Session = SessionLocal()
    try:
        # Find businesses missing crucial data
        businesses = db.query(Business).filter(
            (Business.email == None) | (Business.email == "") | (Business.email == "-") |
            (Business.website == None) | (Business.website == "")
        ).all()

        logger.info(f"Found {len(businesses)} records to enrich.")

        count = 0
        for biz in businesses:
            if fill(biz):
                count += 1
            if count % 10 == 0:
                db.commit()

        db.commit()
        logger.info(f"Finished genuine enrichment. Updated {count} records.")

    except Exception as e:
        logger.error(f"Error during enrichment: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    main()
