import os
import sys
import logging
import time
import re
import requests
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add backend directory to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)

from database import SessionLocal, engine
from models.business import Business
from services.smart_scraper import _clean_url

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(backend_dir, "website_enrichment_v3.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("WebsiteEnrichmentV3")

REGION_DISPLAY = {
    # US
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
    # UK
    "ENG": "England", "SCT": "Scotland", "WLS": "Wales", "NIR": "Northern Ireland",
    # UAE
    "DXB": "Dubai", "AUH": "Abu Dhabi", "SHJ": "Sharjah",
    "AJM": "Ajman", "RAK": "Ras Al Khaimah", "FUJ": "Fujairah", "UAQ": "Umm Al Quwain",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}

DIRECTORY_DOMAINS = [
    "hidubai.com", "192.com", "endole.co.uk", "companyhub.nz", 
    "traderegistry.ae", "uaeplusplus.com", "u.ae", "yellowpages", 
    "yelp.com", "facebook.com", "linkedin.com", "instagram.com", 
    "twitter.com", "youtube.com", "wikipedia.org", "bloomberg.com", 
    "crunchbase.com", "zoominfo.com", "opencorporates.com", 
    "dnb.com", "apollo.io", "allbiz.com", "bizapedia.com",
    "b2bhint.com", "bizstanding.com", "incfact.com", "yasabe.com",
    "manta.com", "chamberofcommerce.com", "bbb.org", "glassdoor.com",
    "nike.ae", "sharafdg.com", "english-heritage.org.uk", "barkershoes.com",
    "amazon", "ebay", "noon.com", "dubizzle.com", "bayut.com", "propertyfinder.ae",
    "clutch.co", "goodfirms.co", "sortlist.com", "themanifest.com"
]

def is_directory_or_social(url: str) -> bool:
    if not url: return False
    domain = urlparse(url).netloc.lower()
    # Check for exact matches or subdomains
    for d in DIRECTORY_DOMAINS:
        if d in domain:
            return True
    # Also block overly long paths that look like directory entries
    path = urlparse(url).path
    if len(path.split('/')) > 4:
        return True
    return False

def get_homepage(url: str) -> str:
    """Convert a subpage URL to its homepage."""
    if not url: return ""
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))

def verify_website(url: str, company_name: str, state: str = "", country: str = "") -> bool:
    """Strictly verify if the URL belongs to the company and is a real homepage."""
    if not url: return False
    if is_directory_or_social(url): return False
    
    # Homepage check: path should be empty or just /
    parsed = urlparse(url)
    if parsed.path and parsed.path not in ["/", ""]:
        # Allow some common homepage-like paths
        if parsed.path.lower() not in ["/index.html", "/home", "/en", "/ar"]:
            return False

    try:
        r = requests.get(url, timeout=12, headers=HEADERS, verify=False, allow_redirects=True)
        if r.status_code == 200:
            final_url = r.url.lower()
            if is_directory_or_social(final_url): return False
            
            # Geo check: if looking for US, don't accept .ca, .uk, etc.
            if country.upper() == "US" and (".ca" in final_url or ".uk" in final_url or ".au" in final_url):
                return False
            if country.upper() == "UAE" and (".us" in final_url or ".uk" in final_url):
                return False

            content = r.text[:30000].lower()
            
            # Parking page detection
            parking_clues = ["domain for sale", "parked free", "buy this domain", "related searches", "parking page", "this domain is for sale"]
            if any(clue in content for clue in parking_clues):
                return False
                
            # Branding check
            name_parts = re.split(r'[\s,&]+', company_name.lower())
            generic_words = {"llc", "ltd", "inc", "corp", "group", "holdings", "limited", "company", "services", "solutions", "international", "fze", "fzco", "psc", "est", "associates", "partners", "ventures", "technologies", "trading", "industries", "holdings", "enterprises", "solutions", "limited"}
            significant_parts = [p for p in name_parts if p not in generic_words and len(p) > 2]
            
            if not significant_parts:
                return True # Can't verify further
                
            # Require a high match rate for multi-word names
            matches = sum(1 for part in significant_parts if part in content)
            
            # If name is "Nova Holdings", matches must be 1/1. 
            # If name is "Blue Technologies Inc", matches must be 1/1.
            # If name is "Acme Blue Group", matches must be at least 1/2.
            threshold = 1.0 if len(significant_parts) <= 2 else 0.6
            
            if matches < len(significant_parts) * threshold:
                # Check title specifically as fallback
                title_match = re.search(r'<title>(.*?)</title>', r.text, re.I)
                if title_match:
                    title = title_match.group(1).lower()
                    title_matches = sum(1 for part in significant_parts if part in title)
                    if title_matches < len(significant_parts) * 0.5:
                        return False
                else:
                    return False
            
            # Geography Check
            if state or country:
                geo_keywords = []
                if state: geo_keywords.append(state.lower())
                if country: geo_keywords.append(country.lower())
                
                # Broaden geo keywords for US/UK/UAE
                if country.upper() == "US": geo_keywords.extend(["united states", "usa"])
                if country.upper() == "UK": geo_keywords.extend(["united kingdom", "britain"])
                if country.upper() == "UAE": geo_keywords.extend(["emirates", "dubai", "abu dhabi"])
                
                if not any(geo in content for geo in geo_keywords):
                    # Some companies don't put their state on the home page, but they should have the country
                    if country and not any(c in content for c in [country.lower(), "united"]):
                        return False
                        
            return True
        return False
    except Exception:
        return False

def find_official_website(company_name: str, city: str = "", state: str = "", country: str = "") -> str:
    """Perform deep web research to identify the correct official URL."""
    try:
        from ddgs import DDGS
        location = f"{city} {state} {country}".strip()
        queries = [
            f'"{company_name}" {location} official website',
            f'"{company_name}" {state} {country} homepage'
        ]
        
        # If UAE, try .ae specifically
        if country.upper() == "UAE":
            queries.insert(0, f'"{company_name}" site:.ae')
            
        with DDGS() as ddgs:
            for query in queries:
                try:
                    results = list(ddgs.text(query, max_results=6))
                    for r in results:
                        url = r.get("href")
                        if not url: continue
                        url = _clean_url(url)
                        hp = get_homepage(url)
                        if hp and verify_website(hp, company_name, state, country):
                            return hp
                except Exception:
                    continue
    except Exception:
        pass
        
    return ""

def process_business(biz_id):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz: return False

        original_url = biz.website
        current_status = "STAY"
        new_url = ""

        # Priority 1: Clean and verify existing
        cleaned_url = _clean_url(original_url) if original_url else ""
        hp_candidate = get_homepage(cleaned_url) if cleaned_url else ""
        
        state_name = REGION_DISPLAY.get(biz.state, biz.state) if biz.state else ""
        
        # If current URL is a directory or explicitly wrong, we MUST replace it
        if not hp_candidate or is_directory_or_social(hp_candidate):
            logger.info(f"[{biz_id}] {biz.company_name}: Bad/Missing link. Searching...")
            new_url = find_official_website(biz.company_name, biz.city or "", state_name, biz.country or "")
            current_status = "REPLACED" if new_url else "FAILED_DISCOVERY"
        else:
            # Check if current homepage is functional and branded
            if verify_website(hp_candidate, biz.company_name, state_name, biz.country or ""):
                if hp_candidate.lower() != original_url.lower().rstrip("/"):
                    new_url = hp_candidate
                    current_status = "CLEANED"
                else:
                    current_status = "VERIFIED"
            else:
                logger.info(f"[{biz_id}] {biz.company_name}: Current link {original_url} unverified. Searching...")
                new_url = find_official_website(biz.company_name, biz.city or "", state_name, biz.country or "")
                current_status = "REPLACED" if new_url else "DEAD/UNVERIFIED"

        if new_url and new_url.lower() != original_url.lower().rstrip("/"):
            biz.website = new_url
            db.commit()
            logger.info(f"[{biz_id}] UPDATED: {biz.company_name} | {original_url} -> {new_url} ({current_status})")
            return True
        else:
            # logger.info(f"[{biz_id}] {current_status}: {biz.company_name} | {original_url}")
            return False

    except Exception as e:
        logger.error(f"[{biz_id}] Error: {e}")
        return False
    finally:
        db.close()

def run_enrichment():
    db = SessionLocal()
    try:
        # Get all business IDs
        # For testing, we might want to limit this initially, but the user asked for 100%
        biz_ids = [r[0] for r in db.query(Business.id).all()]
        total = len(biz_ids)
        
        logger.info(f"Starting Website Accuracy Audit for {total} records...")
        
        updated_count = 0
        with ThreadPoolExecutor(max_workers=5) as executor: # Keep workers low for rate limiting
            futures = [executor.submit(process_business, bid) for bid in biz_ids]
            for i, future in enumerate(futures, 1):
                if future.result():
                    updated_count += 1
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{total} (Updated: {updated_count})")
                    
        logger.info(f"Finished Audit. Total records updated: {updated_count}/{total}")
    finally:
        db.close()

if __name__ == "__main__":
    run_enrichment()
