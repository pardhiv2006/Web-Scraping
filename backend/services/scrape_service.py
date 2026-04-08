
import logging
import re
import threading
from sqlalchemy.orm import Session
from typing import List, Dict

from database import SessionLocal
from models.business import Business
from scrapers.us_scraper import scrape_us
from scrapers.uk_scraper import scrape_uk
from scrapers.uae_scraper import scrape_uae
from services.smart_scraper import smart_extract, discover_company_info

logger = logging.getLogger(__name__)

def run_scrape(country: str, states: List[str], db: Session) -> Dict:
    """
    1. Scrapes basic genuine records from registry
    2. Synchronously discovers and verifies their website
    3. ONLY inserts them if they have a working website to guarantee 100% genuine records.
    """
    all_records = []

    if country == "US": all_records = scrape_us(states)
    elif country == "UK": all_records = scrape_uk(states)
    elif country == "UAE": all_records = scrape_uae(states)
    else: raise ValueError(f"Unsupported country: {country}")

    # Fallback Discovery Layer: Ensure at least one record per requested state
    found_states = set(r.get("state") for r in all_records if r.get("state"))
    missing_states = [s for s in states if s not in found_states]
    
    if missing_states:
        from services.discovery_service import discover_businesses_in_region
        for state in missing_states:
            discovered = discover_businesses_in_region(country, state)
            all_records.extend(discovered)

    inserted_ids = []
    skipped_count = 0
    error_count = 0
    no_website_count = 0

    for record in all_records:
        try:
            rec_reg = (record.get("registration_number") or "").strip()
            existing = db.query(Business).filter(
                Business.registration_number == rec_reg,
                Business.country == country.upper()
            ).first()

            if existing:
                skipped_count += 1
                continue

            company_name = record.get("company_name", "").strip()
            
            new_biz = Business(
                company_name=company_name,
                registration_number=rec_reg,
                country=country.upper(),
                state=(record.get("state") or "").strip().upper(),
                status=record.get("status"),
                source_url=record.get("source_url"),
                registration_date=record.get("registration_date"),
                address=record.get("address")
            )
            db.add(new_biz)
            db.flush()
            inserted_ids.append(new_biz.id)
            
        except Exception as e:
            logger.error(f"Error inserting {record.get('company_name')}: {e}")
            error_count += 1

    db.commit()
    logger.info(f"Scrape Complete. Inserted {len(inserted_ids)} base records. Spawning background threads for enrichment...")
    
    # Fire off background workers to fetch websites & enrich concurrently
    for bid in inserted_ids:
        threading.Thread(target=enrich_business_background, args=(bid,), daemon=True).start()


    return {
        "total_fetched": len(all_records),
        "inserted": len(inserted_ids),
        "skipped_dupes": skipped_count,
        "dropped_no_website": no_website_count,
        "errors": error_count,
        "country": country,
        "states": states
    }
    
def enrich_business_background(business_id: int):
    """
    Background job to re-enrich a specific business.
    This enables targeted deep searches for previously blank fields.
    """
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == business_id).first()
        if not biz:
            return

        company_name = biz.company_name or ""
        website = biz.website or ""
        
        # If there's no website yet, try to discover it
        if not website:
            info = discover_company_info(company_name, biz.state or "", biz.country or "")
            website = info.get("website")
            if website:
                biz.website = website
                
        # Only proceed to deep enrichment if we have a website or company name
        if website:
            extracted = smart_extract(website, company_name=company_name, country=biz.country or "US")
            
            # Update fields that are currently missing
            if not biz.email and extracted.get("email"): biz.email = extracted.get("email")
            if not biz.phone and extracted.get("phone"): biz.phone = extracted.get("phone")
            if not biz.ceo_name and extracted.get("ceo_name"): biz.ceo_name = extracted.get("ceo_name")
            if not biz.linkedin_url and extracted.get("linkedin_url"): biz.linkedin_url = extracted.get("linkedin_url")
            if not biz.description and extracted.get("description"): biz.description = extracted.get("description")
            if not biz.industry and extracted.get("industry"): biz.industry = extracted.get("industry")
            if not biz.employee_count and extracted.get("employee_count"): biz.employee_count = extracted.get("employee_count")
            if not biz.revenue and extracted.get("revenue"): biz.revenue = extracted.get("revenue")
            if not biz.address and extracted.get("address"): biz.address = extracted.get("address")
            
            db.commit()
            logger.info(f"Background enrichment completed for business {biz.id}: {company_name}")
        else:
            logger.info(f"Skipping background enrichment for {biz.id} - no valid website found.")
    except Exception as e:
        logger.error(f"Error in background enrichment for business {business_id}: {e}")
        db.rollback()
    finally:
        db.close()
