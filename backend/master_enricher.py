import os
import sys
import logging
import time
import re
import random
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session
from sqlalchemy import or_

# Add backend to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)

from database import SessionLocal
from models.business import Business
from services.smart_scraper import smart_extract, discover_company_info, _is_valid_name, ADDRESS_RE, _ai_research

# --- Junk Detection Logic (Mirrored from cleanup_bad_data.py) ---
def is_junk_address(addr: str) -> bool:
    if not addr: return False
    addr_lower = addr.lower()
    junk_keywords = [
        'trial aims', 'bhp', 'parent company', 'iso 9001', 
        'privacy & cookies', 'request a quote', 'workspace interiors',
        'document management', 'digital printing', 'entity #',
        'limited liability partnership', 'department of state',
        'obtained a listing', 'london stock exchange', 'dec 16, 2025',
        'established 1997', 'specialist contractor', 'bounces back',
        'proposed to', 'high-profile cases', 'million loss', 'ksh',
        'contact us', 'all rights reserved', 'terms of service', 'amazon\'s',
        'below is a list', 'retail locations', 'view contact profiles',
        'sic code', 'naics code', 'show more', 'popular searches',
        'global inc', 'pte ltd', 'earlier forensics', 'headquartered in washington'
    ]
    if any(kw in addr_lower for kw in junk_keywords): return True
    if len(addr) > 250: return True
    # If it's a long string with no numbers, it's probably not a real address
    if len(addr) > 50 and not any(char.isdigit() for char in addr): return True
    return False

def is_junk_city(city: str) -> bool:
    if not city: return False
    city_lower = city.lower()
    placeholders = ['eng', 'sct', 'dxb', 'auh', 'local region', 'nir', 'wales', 'shj', 'as of', 'ltd', 'inc', 'corp', 'dec 16', 'unknown']
    if city_lower.strip() in placeholders: return True
    if len(city) < 2 or len(city) > 40: return True
    # Cities shouldn't have too many words
    if len(city.split()) > 3: return True
    # Postcode check
    if re.match(r'^[A-Z0-9]{2,4}\s?[A-Z0-9]{3}$', city.upper()): return True
    return False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(backend_dir, "master_enrichment.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MasterEnricher")

# --- Logic for City Extraction ---
def extract_city(address: str, company_name: str = "") -> Optional[str]:
    if not address or is_junk_address(address):
        return None
    
    parts = [p.strip() for p in address.split(",")]
    # Try to find a part that looks like a city name (not generic country labels)
    for part in parts:
        if is_junk_city(part): continue
        # City usually doesn't have numbers
        if any(c.isdigit() for c in part): continue
        if len(part) > 2 and len(part) < 30:
            return part
            
    return None

def extract_domain(url: str) -> Optional[str]:
    if not url: return None
    from urllib.parse import urlparse
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain

# --- Enrichment Logic ---
def enrich_single_business(biz_id: int):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False
        
        logger.info(f"Enriching: {biz.company_name} (ID: {biz.id})")
        
        updated = False
        
        # 1. Basic Discovery (Website, LinkedIn, etc.)
        if not biz.website or not biz.linkedin_url:
            discovery = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
            if discovery.get("website") and not biz.website:
                biz.website = discovery["website"]
                updated = True
            if discovery.get("linkedin_url") and not biz.linkedin_url:
                biz.linkedin_url = discovery["linkedin_url"]
                updated = True
        
        # 2. Deep Scrape from Website
        if biz.website:
            scraped = smart_extract(biz.website, company_name=biz.company_name, country=biz.country or "US")
            
            # Map fields safely with validation
            fields_to_map = {
                "email": "email",
                "phone": "phone",
                "linkedin_url": "linkedin_url",
                "ceo_name": "ceo_name",
                "industry": "industry",
                "description": "description",
                "employee_count": "employee_count",
                "revenue": "revenue",
                "address": "address"
            }
            
            for scraped_key, biz_attr in fields_to_map.items():
                val = scraped.get(scraped_key)
                if val and not getattr(biz, biz_attr):
                    # Validation
                    if biz_attr == "address" and is_junk_address(val): continue
                    if biz_attr == "city" and is_junk_city(val): continue
                    if biz_attr == "ceo_name" and len(val) < 3: continue
                    
                    setattr(biz, biz_attr, val)
                    updated = True
                    
        # 3. Specific LinkedIn Search if still missing details
        if not biz.linkedin_url:
            # Try harder for LinkedIn
            from ddgs import DDGS
            try:
                with DDGS() as ddgs:
                    q = f'"{biz.company_name}" {biz.state or ""} {biz.country or ""} LinkedIn company page'
                    for r in ddgs.text(q, max_results=3):
                        href = r.get("href", "")
                        if "linkedin.com/company/" in href:
                            biz.linkedin_url = href.split("?")[0].rstrip("/")
                            updated = True
                            break
            except Exception as e:
                logger.debug(f"LinkedIn search error: {e}")

        # 4. Mandatory Field Completion & Fallbacks
        
        # Address/City Validation & Junk Reset
        if biz.address and is_junk_address(biz.address):
            logger.info(f"Clearing junk address for ID {biz.id}")
            biz.address = None
            updated = True
            
        if biz.city and is_junk_city(biz.city):
            logger.info(f"Clearing junk city for ID {biz.id}")
            biz.city = None
            updated = True

        if biz.address and not biz.city:
            biz.city = extract_city(biz.address, biz.company_name)
            if biz.city: updated = True
            
        # 5. Deep AI Research as Last Resort for incomplete records
        if not biz.industry or not biz.revenue or not biz.employee_count or not biz.address:
            logger.info(f"Using Deep AI Research for {biz.company_name}...")
            ai_data = _ai_research(biz.company_name, state=biz.state or "", country=biz.country or "US")
            
            mapping = {
                "industry": "industry",
                "revenue": "revenue",
                "employee_count": "employee_count",
                "address": "address",
                "city": "city",
                "ceo_name": "ceo_name",
                "website": "website",
                "phone": "phone",
                "email": "email",
                "registration_date": "registration_date"
            }
            
            for ai_key, biz_attr in mapping.items():
                val = ai_data.get(ai_key)
                if val and not getattr(biz, biz_attr):
                    # For address/city, still validate
                    if biz_attr == "address" and is_junk_address(val): continue
                    if biz_attr == "city" and is_junk_city(val): continue
                    
                    setattr(biz, biz_attr, val)
                    updated = True

        if updated:
            db.commit()
            logger.info(f"Enriched: {biz.company_name} (ID: {biz.id})")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Error processing {biz_id}: {e}")
        return False
    finally:
        db.close()

def run_batch(limit=50, workers=5):
    db = SessionLocal()
    try:
        # Find businesses with ANY missing mandatory field
        query = db.query(Business).filter(
            or_(
                Business.email == None, Business.email == "",
                Business.phone == None, Business.phone == "",
                Business.website == None, Business.website == "",
                Business.linkedin_url == None, Business.linkedin_url == "",
                Business.city == None, Business.city == "",
                Business.industry == None, Business.industry == "",
                Business.employee_count == None, Business.employee_count == "",
                Business.revenue == None, Business.revenue == "",
                Business.description == None, Business.description == ""
            )
        )
        businesses = query.order_by(Business.id.desc()).limit(limit).all()
        total = len(businesses)
        logger.info(f"Starting enrichment for batch of {total} businesses.")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(enrich_single_business, [b.id for b in businesses]))
            
        logger.info("Batch completed.")
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    
    run_batch(limit=args.limit, workers=args.workers)
