import os
import sys
import logging
import time
import requests
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from sqlalchemy import or_, and_

# Ensure backend directory is on the path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.smart_scraper import (
    EMAIL_RE, PHONE_RE, ADDRESS_RE, HEADERS, 
    _clean_address_logic, _is_valid_phone, _is_placeholder_email,
    CONTACT_LABELS
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TargetedScraper")

def fetch_page(url: str, timeout: int = 15) -> str:
    """Fetch HTML content from a URL safely."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, verify=False, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return ""

def extract_from_html(html: str, company_name: str) -> dict:
    """Extract standard contact information from raw HTML text."""
    soup = BeautifulSoup(html, "html.parser")
    # Remove scripts and styles for cleaner text
    for script in soup(["script", "style", "nav", "footer"]):
        script.extract()
        
    text = soup.get_text(separator=' ', strip=True)
    
    result = {"email": None, "phone": None, "address": None}

    # 1. Extract Emails
    for match in EMAIL_RE.finditer(text):
        e = match.group().strip()
        if not _is_placeholder_email(e, company_name):
            result["email"] = e
            break
            
    # 1.5 Extract Emails from mailto:
    if not result["email"]:
        for a in soup.find_all(href=True):
            if a['href'].startswith('mailto:'):
                e = a['href'].replace('mailto:', '').split('?')[0].strip()
                if EMAIL_RE.match(e) and not _is_placeholder_email(e, company_name):
                    result["email"] = e
                    break

    # 2. Extract Phones
    for match in PHONE_RE.finditer(text):
        p = match.group().strip()
        if _is_valid_phone(p):
            result["phone"] = p
            break
            
    # 3. Extract Addresses
    for match in ADDRESS_RE.finditer(text):
        a_raw = match.group().strip()
        cleaned = _clean_address_logic(a_raw)
        # Ensure it has numbers and characters (not just generic string)
        if cleaned and len(cleaned) > 10 and any(c.isdigit() for c in cleaned):
            result["address"] = cleaned
            break
            
    return result, soup

def targeted_scrape_single(biz_id: int):
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == biz_id).first()
        if not biz or not biz.website:
            return False

        updated = False
        target_url = biz.website
        if not target_url.startswith("http"):
            target_url = "https://" + target_url

        logger.info(f"Targeted Direct crawl: {biz.company_name} @ {target_url}")
        
        # 1. Fetch Homepage
        html = fetch_page(target_url, timeout=10)
        ext, soup = extract_from_html(html, biz.company_name)
        
        # 2. Identify Contact Pages if missing data
        needs_deep = not (ext["email"] and ext["phone"] and ext["address"])
        contact_urls = []
        
        if needs_deep and soup:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text().strip()
                if CONTACT_LABELS.search(href) or CONTACT_LABELS.search(text):
                    full_link = urllib.parse.urljoin(target_url, href)
                    if full_link not in contact_urls and full_link.startswith("http"):
                        contact_urls.append(full_link)
                        
        # 3. Fetch Contact Pages
        for curl in contact_urls[:2]:  # Limit to 2 contact pages max
            if ext["email"] and ext["phone"] and ext["address"]:
                break
                
            c_html = fetch_page(curl, timeout=8)
            c_ext, _ = extract_from_html(c_html, biz.company_name)
            
            if not ext["email"]: ext["email"] = c_ext["email"]
            if not ext["phone"]: ext["phone"] = c_ext["phone"]
            if not ext["address"]: ext["address"] = c_ext["address"]

        # 4. Save results
        if ext["email"] and not biz.email:
            biz.email = ext["email"]
            updated = True
        if ext["phone"] and not (biz.phone and _is_valid_phone(biz.phone)):
            biz.phone = ext["phone"]
            updated = True
        if ext["address"] and not biz.address:
            biz.address = ext["address"]
            updated = True
            
        if updated:
            db.commit()
            logger.info(f"[SUCCESS] Scraped {biz.company_name}: E:{bool(ext['email'])} P:{bool(ext['phone'])} A:{bool(ext['address'])}")
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"Error direct scraping ID {biz_id}: {e}")
        db.rollback()
        return False
    finally:
        db.close()

def main(limit=1000, workers=15):
    db = SessionLocal()
    try:
        # We target businesses that HAVE a website, but MISSING address OR email OR phone
        query = db.query(Business.id).filter(
            and_(
                Business.website != None,
                Business.website != "",
                or_(
                    Business.address == None, Business.address == "",
                    Business.email == None, Business.email == "",
                    Business.phone == None, Business.phone == ""
                )
            )
        )
        biz_ids = [r[0] for r in query.limit(limit).all()]
        total = len(biz_ids)
        
        if total == 0:
            logger.info("No records found with known websites and missing data. Cleaning done!")
            return
            
        logger.info(f"Starting TARGETED DIRECT SCRAPE for {total} websites with {workers} workers...")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(targeted_scrape_single, biz_ids))
            count = sum(1 for r in results if r)
            
        logger.info(f"Targeted Scrape Finished. Enriched {count}/{total} websites directly.")
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=15)
    args = parser.parse_args()
    main(limit=args.limit, workers=args.workers)
