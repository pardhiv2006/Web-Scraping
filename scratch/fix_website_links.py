import os
import sys
import logging
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.join(os.getcwd(), "backend"))
from database import SessionLocal
from models.business import Business
from services.smart_scraper import discover_company_info, JUNK_DOMAINS
from services.enrichment_service import enrich_business



from concurrent.futures import ThreadPoolExecutor

def process_one_biz(biz_id):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False
        
        # Use discover_company_info which now has stricter similarity checks
        info = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
        new_url = info.get("website")
        
        if new_url and new_url != biz.website:
            if any(junk in new_url.lower() for junk in JUNK_DOMAINS):
                return False
            
            biz.website = new_url
            db.commit() # Save the website first
            
            # NOW: Deep Enrichment for newly found website
            # This will crawl the site and use AI to get real Email/Phone/CEO
            print(f"    [Deep Enrichment] Researching new site: {new_url} for {biz.company_name}")
            enrich_business(db, biz.id)
            return True

    except Exception as e:
        logger.error(f"Error processing {biz_id}: {e}")
    finally:
        db.close()
    return False

def fix_websites():
    db = SessionLocal()
    try:
        targets = db.query(Business).filter(or_(
            Business.website == None,
            Business.website == "",
            Business.website == "-",
            Business.website == "Not Available",
            Business.website.contains("google.com/search"),
            *[Business.website.contains(kw) for kw in [
                "opencorporates.com", "bizapedia.com", "yelp.com", "yellowpages.com", 
                "bloomberg.com", "chamberofcommerce.com", "find-and-update.company-information",
                "gov.uk", "directory", "listing"
            ]]
        )).all()

        biz_ids = [b.id for b in targets]
        print(f"Found {len(biz_ids)} records to fix. Starting parallel processing (20 threads)...")
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(process_one_biz, biz_ids))




        
        fixed_count = sum(1 for r in results if r)
        print(f"Finished! Fixed {fixed_count} website links.")

    finally:
        db.close()


if __name__ == "__main__":
    fix_websites()
