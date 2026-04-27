"""
Enrichment service — Finds and scrapes company websites for extra data.
Includes a Zero-Tolerance Completion Layer to ensure no blank fields exist.
"""
import logging
import re
import os
import time
import random
import json
import requests
import warnings
from urllib.parse import urlparse, urljoin, quote_plus
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import Optional, Any, List, Dict

from models.business import Business
from services.smart_scraper import smart_extract, find_company_website, discover_company_info

warnings.filterwarnings("ignore", message="Unverified HTTPS")
logger = logging.getLogger(__name__)

# API Keys from environment (optional)
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")

def normalize_country(c: str) -> str:
    if not c: return c
    c = c.strip().upper()
    mapping = {
        "UNITED ARAB EMIRATES": "UAE",
        "UNITED KINGDOM": "UK",
        "GREAT BRITAIN": "UK",
        "US": "USA",
        "UNITED STATES": "USA",
        "UNITED STATES OF AMERICA": "USA"
    }
    return mapping.get(c, c)

def extract_domain(url: str) -> str:
    if not url: return ""
    try:
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        parsed = urlparse(url)
        domain = parsed.netloc or ""
        return domain.lstrip("www.")
    except:
        return ""

def enrich_business(db: Session, business_id: int) -> bool:
    """
    Orchestrates the 5-step enrichment pipeline with a Zero-Tolerance final layer.
    """
    biz = db.query(Business).filter(Business.id == business_id).first()
    if not biz: return False

    updated = False

    # STEP 1: Discovery via Snippets/Search
    if not biz.website or biz.website == "-":
        logger.info(f"[Step1] Discovering website for: {biz.company_name}")
        info_discovered = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
        
        if info_discovered.get("website"):
            biz.website = info_discovered["website"]
            updated = True
        
        # Merge other quick snippet findings
        for field in ["phone", "email", "linkedin_url", "ceo_name"]:
            val = info_discovered.get(field)
            if val and not getattr(biz, field):
                setattr(biz, field, val)
                updated = True

    # STEP 2 & 3: Deep Website Scraping
    if biz.website and biz.website not in ["-", "Not Available"]:
        try:
            logger.info(f"[Step2] Scraping website: {biz.website}")
            info = smart_extract(biz.website, company_name=biz.company_name)
            
            fields_to_merge = [
                "email", "phone", "linkedin_url", "ceo_name", "founder_name", 
                "ceo_email", "description", "industry", "employee_count", "revenue", "address"
            ]
            for f in fields_to_merge:
                val = info.get(f)
                if val and (not getattr(biz, f) or str(getattr(biz, f)).strip().lower() in ["", "-", "none", "null", "not available"]):
                    setattr(biz, f, val)
                    updated = True

        except Exception as e:
            logger.error(f"Scrape error for {biz.company_name}: {e}")

    # STEP 4: FINAL COMPLETION LAYER (Zero-Tolerance)
    # Ensure EVERY column in the database has non-blank, non-placeholder data.
    all_target_fields = [
        "email", "phone", "website", "ceo_name", "linkedin_url", "industry", 
        "description", "employee_count", "revenue", "address", "registration_date",
        "founder_name", "ceo_email", "source_url", "status"
    ]
    
    remaining_blanks = [
        f for f in all_target_fields
        if not getattr(biz, f) or str(getattr(biz, f)).strip().lower() in ["", "-", "none", "null", "not available"]
    ]


    if remaining_blanks:
        logger.info(f"[Research] Filling {len(remaining_blanks)} fields for {biz.company_name}...")
        from services.smart_scraper import _ai_research
        ai_data = _ai_research(biz.company_name, state=biz.state or "", country=biz.country or "US")
        
        for field in remaining_blanks:
            val = ai_data.get(field)
            if val and str(val).lower() not in ["n/a", "unknown", "none", "null", "not available"]:
                setattr(biz, field, str(val))
                updated = True
            else:
                # If AI also fails, set as "Not Available" for primary fields, or leave empty for secondary
                if field in ["email", "phone", "website", "ceo_name"]:
                    if not getattr(biz, field):
                        setattr(biz, field, "Not Available")
                        updated = True
                elif field == "status":
                    setattr(biz, field, "Active")
                    updated = True
                elif field == "source_url":
                    if not biz.website or biz.website == "Not Available":
                        setattr(biz, field, "https://www.google.com/search?q=" + quote_plus(biz.company_name))
                    else:
                        setattr(biz, field, biz.website)
                    updated = True

    if updated:
        db.commit()
        db.refresh(biz)
        logger.info(f"[Done] Enriched: {biz.company_name}")

    
    return updated
