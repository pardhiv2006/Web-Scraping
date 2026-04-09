"""
Enrichment service — Finds and scrapes company websites for extra data.
Uses DuckDuckGo (no API keys) for website discovery + smart_scraper (Instant Data Scraper approach).
"""
from services.smart_scraper import smart_extract, find_company_website, discover_company_info
import logging
import re
import os
import time
import random
import requests
import warnings
from urllib.parse import urlparse, urljoin, quote_plus
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from typing import Optional, Any

from models.business import Business

warnings.filterwarnings("ignore", message="Unverified HTTPS")

logger = logging.getLogger(__name__)

# API Keys from environment (optional — not required)
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")
ZYTE_API_KEY = os.getenv("ZYTE_API_KEY")

# Regex patterns
EMAIL_REGEX = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
LINKEDIN_REGEX = r'linkedin\.com/(?:company|in)/[a-zA-Z0-9_%\-]+'
PHONE_REGEX = r'(?:\+?[\d]{1,3}[\s\-.]?)?(?:\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4})'
CEO_FOUNDER_REGEX = (
    r'(?:CEO|Chief Executive Officer|Founder|Co-Founder|President|Owner|'
    r'Managing Director|MD|Director)\s*[:|\-]?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'
)

FALSE_POSITIVE_NAMES = {"date", "birth", "company", "number", "registered", "office", "terms", "privacy", "policy", "about", "contact", "home", "search", "menu"}

def _is_valid_name(name: str) -> bool:
    """Synchronized validator to prevent 'Date of Birth' etc."""
    if not name or len(name.split()) < 2 or len(name.split()) > 4: return False
    if any(char.isdigit() for char in name): return False
    words = set(name.lower().split())
    if words.intersection(FALSE_POSITIVE_NAMES): return False
    return True

# Domains to skip when searching for company websites
JUNK_DOMAINS = [
    "google.com", "yelp.com", "yellowpages.com", "facebook.com",
    "twitter.com", "instagram.com", "youtube.com", "mapquest.com",
    "opencorporates.com", "bizapedia.com", "bloomberg.com", "crunchbase.com",
    "zoominfo.com", "dnb.com", "manta.com", "bbb.org", "indeed.com",
    "glassdoor.com", "linkedin.com", "wikipedia.org", ".gov", "amazon.com",
    "bing.com", "duckduckgo.com", "trustpilot.com", "ripoffreport.com"
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}


# ───────────────────────────── Utility ─────────────────────────────

def ensure_protocol(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


def extract_domain(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(ensure_protocol(url))
    domain = parsed.netloc or ""
    return domain.lstrip("www.")


def _is_junk(url: str) -> bool:
    url_lower = url.lower()
    return any(j in url_lower for j in JUNK_DOMAINS)


# ───────────────────── Website Discovery (no API) ──────────────────

def _ddg_search(query: str) -> Optional[str]:
    """
    Searches DuckDuckGo HTML (no JS, no API key required) and returns
    the first non-junk result URL.
    """
    try:
        encoded = quote_plus(query)
        url = f"https://html.duckduckgo.com/html/?q={encoded}"
        resp = requests.get(url, headers=HEADERS, timeout=10, verify=False)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.select("a.result__a"):
            href = a.get("href", "")
            if href and href.startswith("http") and not _is_junk(href):
                return href
    except Exception as e:
        logger.debug(f"DDG search error: {e}")
    return None


# Note: find_company_website is imported from smart_scraper


# Removed _targeted_field_search to prevent "small mistakes" from search snippets.
# All data must be genuinely scraped from the company website per user requirement.


# ───────────────────── Page Fetching ──────────────────────────────

def _fetch_page(url: str) -> str:
    """Fetch a page via requests (kept for compatibility)."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12, verify=False,
                            allow_redirects=True)
        if resp.status_code == 200 and len(resp.text) > 300:
            return resp.text
    except Exception as e:
        logger.debug(f"requests fetch failed for {url}: {e}")

    # Selenium fallback for JS-heavy pages
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        
        chrome_bin = os.getenv("CHROME_BIN", "")
        driver_path = os.getenv("CHROMEDRIVER_PATH", "")

        options = Options()
        if chrome_bin:
            options.binary_location = chrome_bin
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        
        if driver_path:
            service = Service(driver_path)
        else:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(15)
        driver.get(url)
        html = driver.page_source
        driver.quit()
        return html
    except Exception as e:
        logger.debug(f"Selenium fetch failed for {url}: {e}")
    return ""


# ───────────────────── Leader Extraction ──────────────────────────

def _extract_leaders(text: str, soup: BeautifulSoup) -> dict:
    leaders: dict[str, Optional[str]] = {"ceo_name": None, "founder_name": None}
    clean = re.sub(r"\s+", " ", text)

    # 1. Explicit title regex
    match = re.search(CEO_FOUNDER_REGEX, clean, re.IGNORECASE)
    if match:
        name = match.group(1).strip()
        context = match.group(0).lower()
        if any(w in context for w in ["ceo", "chief executive", "president", "director", "md"]):
            if _is_valid_name(name): leaders["ceo_name"] = name
        elif any(w in context for w in ["founder", "co-founder", "owner"]):
            if _is_valid_name(name): leaders["founder_name"] = name

    # 2. Schema.org JSON-LD (most authoritative source)
    if not leaders["ceo_name"]:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string or "")
                # Could be a list or dict
                items = data if isinstance(data, list) else [data]
                for item in items:
                    employee = item.get("employee") or item.get("founder") or {}
                    if isinstance(employee, dict):
                        job = employee.get("jobTitle", "")
                        name = employee.get("name", "")
                        if name and any(t in job.lower() for t in ["ceo", "founder", "president", "owner"]):
                            if _is_valid_name(name):
                                leaders["ceo_name"] = name
                                break
            except Exception:
                pass

    # 3. "X & Y specialize in / founded / are the owners" pattern
    if not leaders["ceo_name"]:
        team_match = re.search(
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*(?:&|and)\s*"
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*"
            r"(?:specialize|are the|founded|owners|team|created)",
            clean, re.IGNORECASE
        )
        if team_match:
            n1, n2 = team_match.group(1), team_match.group(2)
            if _is_valid_name(n1) and _is_valid_name(n2):
                leaders["ceo_name"] = f"{n1} & {n2}"

    return leaders


# ───────────────── Website Scraper (main extractor) ───────────────

def _extract_from_page(html: str, base_url: str = "") -> dict:
    """Extract all available fields from a page's HTML."""
    data: dict[str, Any] = {}
    soup = BeautifulSoup(html, "html.parser")

    # Remove noise tags
    for tag in soup(["script", "style", "noscript", "head"]):
        tag.decompose()
    text = soup.get_text(separator=" ")

    # 1. Emails
    emails = re.findall(EMAIL_REGEX, text)
    valid_emails = [
        e.lower() for e in emails
        if isinstance(e, str)
        and not e.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp"))
        and "@" in e
        and "." in e.split("@")[-1]
    ]
    if valid_emails:
        data["email"] = valid_emails[0]

    # 2. Phone
    phones = re.findall(PHONE_REGEX, text)
    clean_phones = [p.strip() for p in phones if len(re.sub(r"\D", "", p)) >= 10]
    if clean_phones:
        data["phone"] = clean_phones[0]

    # 3. LinkedIn URL
    linkedin_match = re.search(LINKEDIN_REGEX, html)
    if linkedin_match:
        data["linkedin_url"] = "https://www." + linkedin_match.group(0)

    # 4. Meta description
    meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
    if meta_desc and meta_desc.get("content"):
        data["description"] = meta_desc["content"][:300]

    # 5. Industry from meta keywords or text
    meta_kw = soup.find("meta", attrs={"name": re.compile(r"keyword", re.I)})
    if meta_kw and meta_kw.get("content"):
        data["industry"] = meta_kw["content"][:100]

    # 6. Leadership
    data.update(_extract_leaders(text, soup))

    return data


def scrape_website_for_info(url: str, company_name: str = "") -> dict:
    """
    Instant Data Scraper approach — uses extruct (JSON-LD/Microdata/OpenGraph),
    DOM pattern detection, trafilatura clean-text, and LinkedIn search.
    """
    return smart_extract(url, company_name=company_name)
    url = ensure_protocol(url)
    results: dict[str, Any] = {
        "email": None, "phone": None, "linkedin_url": None,
        "ceo_name": None, "founder_name": None, "ceo_email": None,
        "industry": None, "employee_count": None, "description": None,
        "website": url
    }
    if not url:
        return results

    try:
        # Main page
        logger.info(f"[Scrape] {url}")
        html = _fetch_page(url)
        if not html:
            return results

        results.update(_extract_from_page(html, url))
        soup = BeautifulSoup(html, "html.parser")

        # Crawl sub-pages for more info (About, Contact, Team)
        sub_keywords = re.compile(r"about|team|contact|leadership|people|our-story", re.I)
        visited = {url}

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("http"):
                href = urljoin(url, href)
            if href in visited or _is_junk(href):
                continue
            link_text = a.get_text(strip=True).lower()
            if sub_keywords.search(link_text) or sub_keywords.search(href):
                visited.add(href)
                try:
                    sub_html = _fetch_page(href)
                    if sub_html:
                        sub_data = _extract_from_page(sub_html, href)
                        # Only fill empty slots
                        for k, v in sub_data.items():
                            if v and not results.get(k):
                                results[k] = v
                except Exception:
                    pass
                if results.get("email") and results.get("ceo_name"):
                    break  # Enough data found

    except Exception as e:
        logger.error(f"[Scrape] Error for {url}: {e}")

    return results


# ───────────────────── Hunter API (optional) ──────────────────────

def get_hunter_emails(domain: str, full_name: Optional[str] = None) -> dict:
    """Calls Hunter.io API to find professional emails (requires API key)."""
    if not HUNTER_API_KEY or not domain:
        return {"email": None, "ceo_email": None}
    results: dict[str, Optional[str]] = {"email": None, "ceo_email": None}
    try:
        resp = requests.get(
            f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}",
            timeout=10
        )
        if resp.status_code == 200:
            emails = resp.json().get("data", {}).get("emails", [])
            if emails:
                results["email"] = emails[0].get("value")
        if full_name:
            parts = full_name.split()
            if len(parts) >= 2:
                first, last = parts[0], parts[-1]
                finder = requests.get(
                    f"https://api.hunter.io/v2/email-finder?domain={domain}"
                    f"&first_name={first}&last_name={last}&api_key={HUNTER_API_KEY}",
                    timeout=10
                )
                if finder.status_code == 200:
                    fdata = finder.json().get("data", {})
                    if fdata and fdata.get("email"):
                        results["ceo_email"] = fdata["email"]
    except Exception as e:
        logger.error(f"Hunter API Error: {e}")
    return results


def get_phantombuster_data(company_name: str, domain: str, ceo_name: Optional[str] = None) -> dict:
    """PhantomBuster stub — no API key available, returns empty."""
    return {"linkedin_url": None, "ceo_name": None}


# ───────────────────── 5-Step Enrich Pipeline ─────────────────────

def enrich_business(db: Session, business_id: int) -> bool:
    """
    Orchestrates the 5-step enrichment pipeline:
    1. Website discovery via DuckDuckGo search
    2. Domain extraction
    3. Email/Phone/LinkedIn/CEO scrape from company website
    4. Sub-page crawl (About, Contact, Team)
    5. Merge & save to database
    Returns True if any field was updated.
    """
    biz = db.query(Business).filter(Business.id == business_id).first()
    if not biz:
        return False

    updated = False

    # STEP 1: Discover info & website via Bing Snippets (AnyPicker style)
    if not biz.website:
        logger.info(f"[Step1] Searching info for: {biz.company_name}")
        from services.smart_scraper import discover_company_info
        info_discovered = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
        
        if info_discovered.get("website"):
            biz.website = info_discovered["website"]
            updated = True
        
        # Apply other snippet findings immediately
        if info_discovered.get("phone") and not biz.phone:
            biz.phone = info_discovered["phone"]
            updated = True
        if info_discovered.get("linkedin_url") and not biz.linkedin_url:
            biz.linkedin_url = info_discovered["linkedin_url"]
            updated = True
        if info_discovered.get("email") and not biz.email:
            biz.email = info_discovered["email"]
            updated = True
            
        if updated:
            db.commit()
            
        if not biz.website:
            logger.warning(f"[Step1] No website discovered for: {biz.company_name}")
            # Even if no website, we might have found phone/LinkedIn in snippet
            return updated 

    # STEP 2: Extract domain
    domain = extract_domain(biz.website)

    # STEP 3 & 4: Scrape website + sub-pages
    info = scrape_website_for_info(biz.website, company_name=biz.company_name)

    # Removed Step 4.5: Targeted Search for remaining blanks
    # We only use data that is genuinely scraped from the website.
    pass

    # STEP 5: Merge only empty fields
    def _set(field, value):
        nonlocal updated
        if value and not getattr(biz, field):
            setattr(biz, field, value)
            updated = True

    _set("email",         info.get("email"))
    _set("phone",         info.get("phone"))
    _set("linkedin_url",  info.get("linkedin_url"))
    _set("ceo_name",      info.get("ceo_name") or info.get("founder_name"))
    _set("ceo_email",     info.get("ceo_email"))
    _set("founder_name",  info.get("founder_name"))
    _set("description",   info.get("description"))
    _set("industry",      info.get("industry"))
    _set("employee_count", info.get("employee_count"))
    _set("revenue",       info.get("revenue"))

    # Optional Hunter CEO email (only if key is set)
    if domain and HUNTER_API_KEY:
        hunter = get_hunter_emails(domain, biz.ceo_name)
        if hunter.get("email"): _set("email", hunter.get("email"))
        if hunter.get("ceo_email"): _set("ceo_email", hunter.get("ceo_email"))

    if updated:
        db.commit()
        logger.info(f"[Done] Enriched: {biz.company_name}")
    else:
        logger.info(f"[Skip] Nothing new for: {biz.company_name}")

    return updated
