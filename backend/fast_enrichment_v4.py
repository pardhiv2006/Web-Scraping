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
from services.smart_scraper import smart_extract, discover_company_info

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FastEnricherV4")

def fast_enrich_single(biz_id):
    db = SessionLocal()
    # Stagger workers to prevent immediate API bans
    time.sleep(random.uniform(0.5, 3.5))
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False
        
        updated = False
        
        # 1. Discover Website/LinkedIn (Fast)
        if not biz.website or not biz.linkedin_url:
            discovery = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
            if discovery.get("website") and not biz.website:
                biz.website = discovery["website"]
                updated = True
            if discovery.get("linkedin_url") and not biz.linkedin_url:
                biz.linkedin_url = discovery["linkedin_url"]
                updated = True

        # 2. Fast Snippet Extract (No site visit)
        # We pass fast_mode=True to ensure no Selenium/Requests to the target domain
        info = smart_extract(biz.website or "", company_name=biz.company_name, fast_mode=True)
        
        fields = ["email", "phone", "ceo_name", "address", "industry"]
        for f in fields:
            val = info.get(f)
            if val and not getattr(biz, f):
                setattr(biz, f, val)
                updated = True
                
        if updated:
            db.commit()
            logger.info(f"[FAST] Enriched {biz.company_name}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error in fast enrich for {biz_id}: {e}")
        db.rollback()
        return False
    finally:
        db.close()

def main(limit=500, workers=20):
    db = SessionLocal()
    try:
        # Target records with missing critical info
        query = db.query(Business.id).filter(
            or_(
                Business.email == None, Business.email == "",
                Business.phone == None, Business.phone == "",
                Business.ceo_name == None, Business.ceo_name == "",
                Business.address == None, Business.address == ""
            )
        )
        biz_ids = [r[0] for r in query.limit(limit).all()]
        total = len(biz_ids)
        logger.info(f"Starting FAST enrichment for {total} businesses with {workers} workers.")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(fast_enrich_single, biz_ids))
            count = sum(1 for r in results if r)
            
        logger.info(f"FAST Pass Finished. Successfully enriched {count}/{total} records.")
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--workers", type=int, default=20)
    args = parser.parse_args()
    main(limit=args.limit, workers=args.workers)
