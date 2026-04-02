import sys
import os
import logging
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

# Ensure backend directory is on the path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.smart_scraper import discover_company_info

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("WebsiteRepair")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}

def verify_link(url: str) -> bool:
    """Strictly verify if the URL returns a 200 OK and isn't a known dead page."""
    if not url: return False
    try:
        # Use a short timeout to skip truly dead sites fast
        r = requests.get(url, timeout=10, allow_redirects=True, headers=HEADERS, verify=False, stream=True)
        if r.status_code == 200:
            # Check if it's a parked domain or common 'not found' placeholder
            text_peek = r.iter_lines()
            try:
                first_lines = str(next(text_peek)).lower()
                if any(x in first_lines for x in ["domain for sale", "parked free", "404 not found"]):
                    return False
            except StopIteration:
                pass
            return True
        return False
    except Exception:
        return False

def repair_business_website(biz_id):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False

        current_url = biz.website
        
        # Check if currently working
        if verify_link(current_url):
            # logger.info(f"[{biz_id}] OK: {current_url}")
            return False

        logger.warning(f"[{biz_id}] DEAD: {current_url} for {biz.company_name}. Searching for replacement...")
        
        # Try to find a new working website
        # Aggressive search: Company Name + Registration Location
        info = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
        new_url = info.get("website")
        
        if new_url and verify_link(new_url):
            logger.info(f"  [FIXED] {biz.company_name}: {current_url} -> {new_url}")
            biz.website = new_url
            db.commit()
            return True
        else:
            logger.error(f"  [FAILED] Could not find working website for {biz.company_name}")
            return False
            
    except Exception as e:
        logger.error(f"  [ERROR] {biz_id}: {e}")
        return False
    finally:
        db.close()

def run_repair():
    db = SessionLocal()
    try:
        # Get all business IDs
        biz_ids = [r[0] for r in db.query(Business.id).all()]
        total = len(biz_ids)
        
        logger.info(f"Starting dedicated website repair for {total} records...")
        
        fixed_count = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(repair_business_website, bid) for bid in biz_ids]
            for i, future in enumerate(futures, 1):
                if future.result():
                    fixed_count += 1
                if i % 50 == 0:
                    logger.info(f"Progress: {i}/{total} (Fixed: {fixed_count})")
                    
        logger.info(f"Finished. Total websites repaired: {fixed_count}/{total}")
    finally:
        db.close()

if __name__ == "__main__":
    run_repair()
