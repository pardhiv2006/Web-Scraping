import sys
import os
import logging
import time
import requests
from concurrent.futures import ThreadPoolExecutor

# Ensure backend directory is on the path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.smart_scraper import smart_extract, discover_company_info
from services.enrichment_service import extract_domain

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RepairAndEnrich")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}

def is_website_alive(url: str) -> bool:
    """Verifies that the website home page actually opens without 404."""
    if not url: return False
    try:
        # Try HEAD first for speed
        r = requests.head(url, timeout=10, allow_redirects=True, headers=HEADERS, verify=False)
        if r.status_code < 400: return True
        # Some servers block HEAD, try GET
        r = requests.get(url, timeout=12, stream=True, headers=HEADERS, verify=False)
        return r.status_code < 400
    except Exception:
        return False

def process_business(biz_id):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False

        logger.info(f"[{biz_id}] Checking: {biz.company_name} | {biz.website}")
        
        # 1. WEBSITE VALIDATION & REPAIR
        website_ok = is_website_alive(biz.website)
        
        if not website_ok:
            logger.warning(f"  [404/DEAD] {biz.website} - Attempting repair...")
            info = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
            new_website = info.get("website")
            
            if new_website and new_website != biz.website:
                logger.info(f"  [REPAIRED] Found new homepage: {new_website}")
                biz.website = new_website
            else:
                logger.error(f"  [FAILED] Could not repair website for {biz.company_name}")
                # We keep the old one but mark as needing attention or just continue for other fields
        
        # 2. DATA ENRICHMENT (Only if website is now alive)
        if is_website_alive(biz.website):
            # Scrape deep for missing fields
            extracted = smart_extract(biz.website, company_name=biz.company_name)
            
            updated = False
            # Only fill if current field is blank
            if not biz.phone and extracted.get("phone"):
                biz.phone = extracted["phone"]
                updated = True
            if not biz.ceo_name and extracted.get("ceo_name"):
                biz.ceo_name = extracted["ceo_name"]
                updated = True
            if not biz.linkedin_url and extracted.get("linkedin_url"):
                biz.linkedin_url = extracted["linkedin_url"]
                updated = True
            if not biz.email and extracted.get("email"):
                biz.email = extracted["email"]
                updated = True
            if not biz.description and extracted.get("description"):
                biz.description = extracted["description"]
                updated = True
            if not biz.industry and extracted.get("industry"):
                biz.industry = extracted["industry"]
                updated = True
                
            if updated or not website_ok: # Commit if we fixed the website or found new data
                db.commit()
                return True
                
        return False
    except Exception as e:
        logger.error(f"  [ERROR] Processing {biz_id}: {e}")
        return False
    finally:
        db.close()

def run_mass_enrich(limit=None):
    db = SessionLocal()
    try:
        # Get all businesses that either have dead websites or missing fields
        query = db.query(Business.id)
        if limit:
            query = query.limit(limit)
        
        biz_ids = [r[0] for r in query.all()]
        total = len(biz_ids)
        logger.info(f"Starting repair and enrichment for {total} records using 5 parallel workers...")
        
        success_count = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_business, bid) for bid in biz_ids]
            for i, future in enumerate(futures, 1):
                if future.result():
                    success_count += 1
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{total} (Modified: {success_count})")
        
        logger.info(f"Completed. Total modified: {success_count}/{total}")
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run_mass_enrich(limit=args.limit)
