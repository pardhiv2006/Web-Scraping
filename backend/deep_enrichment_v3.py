import os
import sys
import logging
import time
import random
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import or_

# Ensure backend directory is on the path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.smart_scraper import smart_extract, discover_company_info, _is_website_functional

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DeepEnricherV3")

def enrich_single_business(biz_id):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False
        
        updated = False
        
        # 1. Discover Website & LinkedIn
        if not biz.website or not biz.linkedin_url:
            discovery = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
            if discovery.get("website") and not biz.website:
                biz.website = discovery["website"]
                updated = True
            if discovery.get("linkedin_url") and not biz.linkedin_url:
                biz.linkedin_url = discovery["linkedin_url"]
                updated = True
        
        # 2. Scrape Website for extra info
        if biz.website:
            info = smart_extract(biz.website, company_name=biz.company_name)
            
            # Merge fields
            fields = ["email", "phone", "linkedin_url", "ceo_name", "ceo_email", "founder_name", "description", "industry", "employee_count", "address"]
            for f in fields:
                val = info.get(f)
                if val and not getattr(biz, f):
                    setattr(biz, f, val)
                    updated = True
        
        # 3. Deep search for Address/CEO if still missing
        from services.smart_scraper import _find_ceo_via_search, _find_address_via_search, _clean_address_logic
        if not biz.ceo_name:
            ceo = _find_ceo_via_search(biz.company_name, state=biz.state or "")
            if ceo:
                biz.ceo_name = ceo
                updated = True
        
        if not biz.address:
            addr = _find_address_via_search(biz.company_name, state=biz.state or "")
            if addr:
                biz.address = _clean_address_logic(addr)
                updated = True

        if updated:
            db.commit()
            logger.info(f"Enriched {biz.company_name}: Email={bool(biz.email)}, Phone={bool(biz.phone)}, Website={bool(biz.website)}")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Error enriching business {biz_id}: {e}")
        db.rollback()
        return False
    finally:
        db.close()

def main(limit=100, workers=5):
    db = SessionLocal()
    try:
        # Target businesses with missing data
        query = db.query(Business.id).filter(
            or_(
                Business.email == None, Business.email == "",
                Business.phone == None, Business.phone == "",
                Business.website == None, Business.website == "",
                Business.linkedin_url == None, Business.linkedin_url == "",
                Business.address == None, Business.address == ""
            )
        )
        biz_ids = [r[0] for r in query.all()]
        if limit:
            biz_ids = biz_ids[:limit]
            
        total = len(biz_ids)
        logger.info(f"Starting deep enrichment for {total} businesses with {workers} workers.")
        
        count = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(enrich_single_business, biz_ids))
            count = sum(1 for r in results if r)
            
        logger.info(f"Finished. Successfully enriched {count}/{total} businesses.")
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--workers", type=int, default=5)
    args = parser.parse_args()
    main(limit=args.limit, workers=args.workers)
