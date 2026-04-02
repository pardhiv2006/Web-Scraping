import os
import logging
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure we use the correct absolute path to the database
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "businesses.db")
os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"

import random
from models.business import Business
from services.smart_scraper import smart_extract, discover_company_info

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

# List of addresses known to be generic/placeholder or noisy from snippets
GENERIC_ADDRESSES = {
    "Dubai", "Sharjah, UAE", "Abu Dhabi, UAE", "Ajman, UAE", 
    "Ras Al Khaimah, UAE", "Fujairah, UAE", "Umm Al Quwain, UAE",
    "DIFC, Dubai", "Dubai, UAE",
    "Business Area, US", "Business Area, UAE", "Business Area, UK",
    "Business Area, Country Name", "0552 Home FAQs About us"
}
NOISY_PATTERNS = ["90 days", "visa", "applicant", "tourist", "currencies", "bank balance", "FAQs", "Home FAQs"]

def is_placeholder(addr: str) -> bool:
    if not addr: return True
    # Truly absolute generic placeholders
    GENERIC_STRICT = {"Business Area, US", "Business Area, UK", "Business Area, UAE", "Business Area, Country Name", "Dubai", "London", "New York"}
    if addr.strip() in GENERIC_STRICT: return True
    
    # Noise detected from snippets
    if any(p.lower() in addr.lower() for p in NOISY_PATTERNS): return True
    
    # If it's too short and doesn't look like a building number + street, it's risky but let's be careful
    if len(addr) < 10 and not any(c.isdigit() for c in addr): return True
    
    return False

def enrich_single_business(biz_id):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False

        is_invalid_address = is_placeholder(biz.address) or (biz.country == "UAE" and biz.address and biz.address.strip() == (biz.state or "").title())
        missing_fields = not all([biz.email, biz.phone, biz.ceo_name, biz.linkedin_url, biz.website])
        
        if not (is_invalid_address or missing_fields):
            return False

        logger.info(f"Enriching: {biz.company_name} ({biz.country}) [ID: {biz_id}]")
        needs_update = False
        
        company_name = biz.company_name
        website = biz.website
        
        # 1. Discover website if missing
        if not website:
            info = discover_company_info(company_name, biz.state or "", biz.country or "")
            website = info.get("website")
            if website:
                biz.website = website
                needs_update = True
        
        # 2. Extract deep info if we have a website or just via search
        extracted = smart_extract(website or "", company_name=company_name)
        
        # Update fields
        if (not biz.address or is_invalid_address) and extracted.get("address"):
            biz.address = extracted.get("address")
            needs_update = True
        
        if not biz.email and extracted.get("email"):
            biz.email = extracted.get("email")
            needs_update = True
            
        if not biz.phone and extracted.get("phone"):
            biz.phone = extracted.get("phone")
            needs_update = True
            
        if not biz.ceo_name and extracted.get("ceo_name"):
            biz.ceo_name = extracted.get("ceo_name")
            needs_update = True
            
        if not biz.linkedin_url and extracted.get("linkedin_url"):
            biz.linkedin_url = extracted.get("linkedin_url")
            needs_update = True

        if not biz.description and extracted.get("description"):
            biz.description = extracted.get("description")
            needs_update = True

        if needs_update:
            db.commit()
            logger.info(f"COMPLETE: {company_name}")
            return True
        else:
            logger.info(f"NO DATA: {company_name}")
            return False

    except Exception as e:
        logger.error(f"Error enriching {biz_id}: {e}")
        return False
    finally:
        db.close()

def mass_enrich():
    db = SessionLocal()
    parser = argparse.ArgumentParser()
    parser.add_argument("--priority-ids", type=str, help="Comma-separated IDs to enrich first")
    args = parser.parse_args()

    try:
        # Get all business IDs that need enrichment
        all_biz = db.query(Business).all()
        
        priority_ids = []
        if args.priority_ids:
            priority_ids = [int(x) for x in args.priority_ids.split(",") if x.strip()]

        # Priority 1: Specifically requested IDs
        targets_priority = [r.id for r in all_biz if r.id in priority_ids and is_placeholder(r.address)]
        
        # Priority 2: Random UK/UAE (User's likely viewport)
        targets_global = [r.id for r in all_biz if is_placeholder(r.address) and r.country in ["UK", "UAE"] and r.id not in targets_priority]
        
        # Priority 3: USA with websites
        targets_with_web = [r.id for r in all_biz if is_placeholder(r.address) and r.country == "USA" and r.website and r.id not in targets_priority and r.id not in targets_global]
        
        # Priority 4: Rest
        targets_rest = [r.id for r in all_biz if is_placeholder(r.address) and r.id not in targets_priority and r.id not in targets_global and r.id not in targets_with_web]
        
        random.shuffle(targets_global)
        random.shuffle(targets_rest)
        
        targets = targets_priority + targets_global + targets_with_web + targets_rest
        logger.info(f"Starting parallel enrichment for {len(targets)} targets (Priority IDs: {len(targets_priority)}).")
        
        # Use ThreadPoolExecutor for Extreme-Turbo high-speed concurrency
        updated_count = 0
        with ThreadPoolExecutor(max_workers=25) as executor:
            future_to_id = {executor.submit(enrich_single_business, biz_id): biz_id for biz_id in targets}
            for future in as_completed(future_to_id):
                if future.result():
                    updated_count += 1
                    if updated_count % 10 == 0:
                        logger.info(f"Progress: {updated_count} records updated in this session.")
                
        logger.info(f"Mass enrichment complete. Updated {updated_count} records.")

    finally:
        db.close()

if __name__ == "__main__":
    mass_enrich()
