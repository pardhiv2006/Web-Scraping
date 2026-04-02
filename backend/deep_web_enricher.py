import os
import sys
import logging
import time
import re
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.smart_scraper import _is_valid_name

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DeepEnricher")

CEO_RE = re.compile(
    r'(?:CEO|Chief Executive|Founder|Co-Founder|President|Owner|Managing Director|Managing Partner|Principal)\s*[:|\-]?\s*'
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    re.IGNORECASE
)

def search_ddg_native(query, max_results=3):
    from ddgs import DDGS
    results = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                results.append(r)
    except Exception as e:
        logger.debug(f"DDGS error: {e}")
    return results

def process_business(biz_id, company_name, state):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False
        
        updated = False
        
        # 1. Scrape LinkedIn URL
        if not biz.linkedin_url:
            q = f'"{company_name}" {state or ""} LinkedIn company'
            results = search_ddg_native(q, max_results=3)
            for r in results:
                href = r.get("href", "")
                if ("linkedin.com/company/" in href or "linkedin.com/in/" in href) and "/search/" not in href and "/dir/" not in href:
                    clean_href = href.split("?")[0]
                    biz.linkedin_url = clean_href
                    updated = True
                    break
                    
        # 2. Scrape CEO
        if not biz.ceo_name:
            q = f'"{company_name}" {state or ""} CEO OR Founder'
            results = search_ddg_native(q, max_results=5)
            for r in results:
                title = r.get("title", "")
                body = r.get("body", "")
                full_text = f"{title} {body}"
                
                # Check regex
                m = CEO_RE.search(full_text)
                if m:
                    candidate = m.group(1).strip()
                    if _is_valid_name(candidate):
                        biz.ceo_name = candidate
                        updated = True
                        break
                
                # Check title format Name - CEO - Company
                name_match = re.match(r'^([^\|\-]+?)\s+(?:-|\|)\s+(?:CEO|Founder|Owner|President)', title, re.IGNORECASE)
                if name_match:
                    candidate = name_match.group(1).strip()
                    if _is_valid_name(candidate):
                        biz.ceo_name = candidate
                        updated = True
                        break
        
        if updated:
            db.commit()
            logger.info(f"Updated {company_name}: CEO={biz.ceo_name}, LinkedIn={biz.linkedin_url}")
            return True
        return False
        
    finally:
        db.close()

def run_enrichment(limit=0, workers=5):
    db = SessionLocal()
    try:
        query = db.query(Business).filter(
            (Business.linkedin_url == None) | (Business.linkedin_url == "") |
            (Business.ceo_name == None) | (Business.ceo_name == "")
        )
        businesses = query.order_by(Business.id).limit(limit or 10000).all()
        total = len(businesses)
        logger.info(f"Starting deep enrichment for {total} businesses.")
        
        count = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(process_business, b.id, b.company_name, b.state) for b in businesses]
            for i, f in enumerate(futures, 1):
                try:
                    if f.result(): count += 1
                except Exception as e:
                    logger.error(f"Worker error: {e}")
                
                if i % 10 == 0:
                    logger.info(f"Processed {i}/{total}. Updated {count} so far.")
                    time.sleep(2) # Prevent hardcore rate limiting
                    
        logger.info(f"Finished. Successfully enriched {count} businesses.")
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    run_enrichment(limit=args.limit, workers=args.workers)
