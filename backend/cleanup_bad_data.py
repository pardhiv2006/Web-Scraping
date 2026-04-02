import sys
import os
import logging
from sqlalchemy import or_

# Ensure backend directory is on the path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.smart_scraper import _is_valid_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Cleanup")

# List of placeholder phone patterns to null out
PHONES_TO_CLEAN = [
    "+1 000 000 0000",
    "000 000 0000",
    "+971 4 000 0000",
    "+968-1234-5678"
]

ADDRESS_PLACEHOLDERS = [
    "%90 days on each visit%",
    "%this type of tourist visa%",
    "%bank balance of 4,000 USD%",
    "%Dubai, UAE%",
    "%London, UK%",
    "%New York, US%",
    "%Business Area%"
]

JUNK_WEBSITES = [
    "https://www.inkl.com/news%",
    "https://www.ibtimes.com%",
    "https://www.rak.ae%",
    "https://rak.ae%",
    "https://emirates.ae%",
    "https://www.al-majid.com%",
    "https://www.habtoor.com%",
    "https://www.bloomberg.com%",
    "https://www.crunchbase.com%",
    "https://www.zoominfo.com%"
]

def cleanup():
    db = SessionLocal()
    try:
        # 1. Clean Websites
        logger.info("Cleaning junk news/portal websites...")
        total_ws_cleaned = 0
        for pattern in JUNK_WEBSITES:
            count = db.query(Business).filter(Business.website.like(pattern)).update(
                {Business.website: None}, synchronize_session=False
            )
            total_ws_cleaned += count

        # 2. Clean Addresses
        logger.info("Cleaning placeholder phone numbers...")
        phone_count = db.query(Business).filter(Business.phone.in_(PHONES_TO_CLEAN)).update(
            {Business.phone: None}, synchronize_session=False
        )
        
        # Also clean any phone that is just zeros/placeholder-like
        more_phones = db.query(Business).filter(
            or_(
                Business.phone.like("%000 000%"),
                Business.phone.like("%0000000%")
            )
        ).update({Business.phone: None}, synchronize_session=False)

        # 2. Clean Addresses
        logger.info("Cleaning placeholder addresses...")
        total_addr_cleaned = 0
        for pattern in ADDRESS_PLACEHOLDERS:
            count = db.query(Business).filter(Business.address.like(pattern)).update(
                {Business.address: None}, synchronize_session=False
            )
            total_addr_cleaned += count
        
        # 3. Clean CEO names using the smart scraper validation
        logger.info("Cleaning invalid CEO names/titles using _is_valid_name logic...")
        businesses_to_check = db.query(Business).filter(Business.ceo_name != None).all()
        invalid_ceo_count = 0
        
        for biz in businesses_to_check:
            if not _is_valid_name(biz.ceo_name):
                # Update manually if the name is invalid
                biz.ceo_name = None
                invalid_ceo_count += 1
                
        db.commit()
        logger.info(f"Cleanup complete.")
        logger.info(f"  Phone placeholders removed: {phone_count + more_phones}")
        logger.info(f"  Address placeholders removed: {total_addr_cleaned}")
        logger.info(f"  Invalid CEO names removed: {invalid_ceo_count}")
        
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    cleanup()
