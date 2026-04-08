
import os
import sys
import logging
import json
import re
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add backend to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)

from database import SessionLocal
from models.business import Business
from services.smart_scraper import _ai_research, discover_company_info, smart_extract

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("DeepAIEnricherV2")

def standardize_revenue(revenue_str: str, country: str = "US") -> str:
    """Standardize revenue with currency symbol and scale."""
    if not revenue_str or revenue_str.lower() in ["n/a", "unknown", "none"]:
        return ""
    
    # Clean string
    rev = revenue_str.replace(",", "").strip()
    
    # If it's just numbers, add symbol based on country
    if re.fullmatch(r'\d+(?:\.\d+)?', rev):
        symbol = "$" if country.upper() != "IN" else "₹"
        return f"{symbol}{rev}"
    
    # If it has currency but no symbol (e.g. 500 million USD)
    if "USD" in rev.upper() and "$" not in rev:
        rev = rev.upper().replace("USD", "").strip()
        return f"${rev}"
    
    if "INR" in rev.upper() and "₹" not in rev:
        rev = rev.upper().replace("INR", "").strip()
        return f"₹{rev}"
        
    return revenue_str

def enrich_business(biz_id: int):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return
        
        logger.info(f"Targeting: {biz.company_name} (ID: {biz.id})")
        
        # 1. Start with discovery if website/linkedin is missing
        if not biz.website or not biz.linkedin_url:
            discovery = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
            if discovery.get("website") and not biz.website:
                biz.website = discovery["website"]
            if discovery.get("linkedin_url") and not biz.linkedin_url:
                biz.linkedin_url = discovery["linkedin_url"]
        
        # 2. Perform deep AI research (Layer 8)
        # We'll use the existing _ai_research but we'll try to refine the prompt inside this script
        # if the default one is too basic.
        logger.info(f"Running LLM research for {biz.company_name}...")
        res = _ai_research(
            biz.company_name, 
            state=biz.city or biz.state or "", 
            country=biz.country or "US"
        )
        
        # 3. Apply changes with strict validation
        updated = False
        
        fields = ["industry", "revenue", "employee_count", "description", "website", "linkedin_url", "email", "address", "phone"]
        
        for field in fields:
            val = res.get(field)
            current_val = getattr(biz, field)
            
            # Only update if current is empty or looks like a placeholder
            is_empty = not current_val or current_val.lower() in ["n/a", "unknown", "none", "-"]
            
            if val and is_empty:
                # Specific logic for revenue
                if field == "revenue":
                    val = standardize_revenue(val, biz.country or "US")
                
                # Validation: No fabrications (check if it looks like a real email/URL)
                if field == "email" and "@" not in val: continue
                if field == "website" and "http" not in val: continue
                
                setattr(biz, field, val)
                updated = True
        
        if updated:
            db.commit()
            logger.info(f"Successfully enriched {biz.company_name}")
        else:
            logger.info(f"No new data found for {biz.company_name}")
            
    except Exception as e:
        logger.error(f"Error enriching {biz_id}: {e}")
        db.rollback()
    finally:
        db.close()

def main():
    db = SessionLocal()
    # Batch: New companies added since 2026-04-06
    # IDs: 1700, 1702, 1703, 1704, 1705, 1706, 1707
    target_ids = [1700, 1702, 1703, 1704, 1705, 1706, 1707]
    db.close()
    
    logger.info(f"Starting deep enrichment for IDs: {target_ids}")
    for b_id in target_ids:
        enrich_business(b_id)
        
    logger.info("Enrichment run completed.")

if __name__ == "__main__":
    main()
