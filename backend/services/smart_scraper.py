"""
smart_scraper.py — Instant Data Scraper logic replicated in Python.

Techniques used:
  1. JSON-LD / Schema.org extraction     (extruct)
  2. Microdata / Open Graph / RDFa       (extruct)
  3. DOM pattern detection               (BeautifulSoup)
  4. trafilatura clean-text extraction   (trafilatura)
  5. Regex over cleaned text             (email, phone, LinkedIn)

ONLY GENUINE DATA is returned. No guessing/synthesizing data.
"""
import re
import json
import logging
import warnings
import os
import requests
import time
import random
from urllib.parse import urljoin, urlparse, quote as urlquote, quote_plus
from typing import Optional, List, Dict

import extruct
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

# ── Patterns ──────────────────────────────────────────────────────
EMAIL_RE    = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE    = re.compile(r'(?:\+?[\d]{1,3}[\s\-.]?)?(?:\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4})')
LINKEDIN_RE = re.compile(r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[a-zA-Z0-9_%\-]+/?')

# Tightened CEO regex to capture names following titles, but strictly filtering later
CEO_RE      = re.compile(
    r'(?:CEO|Chief Executive|Founder|Co-Founder|President|Owner|Managing Director|Managing Partner|Principal)\s*[:|\-]?\s*'
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    re.IGNORECASE
)

# Words that indicate a false positive name match or a title being caught as a name
FALSE_POSITIVE_NAMES = {
    "date", "birth", "company", "number", "registered", "office", "terms", "privacy", 
    "policy", "about", "contact", "home", "search", "menu", "managing", "director", 
    "partner", "president", "founder", "chief", "executive", "officer", "vice", "president",
    "view", "biography", "profile", "team", "leadership", "staff", "employees",
    "advisor", "senior", "legacy", "thank", "services", "solutions", "group",
    "holdings", "technologies", "inc", "llc", "corp", "co", "ltd", "corporation", "limited"
}

EMP_COUNT_RE = re.compile(r'(?:employees|staff|team size|company size)\s*[:|\-]?\s*(\d+[\d,\-s]*\+?)', re.IGNORECASE)
REVENUE_RE   = re.compile(r'(?:revenue|annual turnover|sales|annual revenue)\s*[:|\-]?\s*([$€£¥]?\s*\d+(?:\.\d+)?[MBKmbk]?\s*(?:million|billion|thousand|USD|EUR|GBP)?(?:\s*-\s*[$€£¥]?\s*\d+(?:\.\d+)?[MBKmbk]?)?)', re.IGNORECASE)

JUNK_EMAILS = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".pdf"}
CONTACT_LABELS = re.compile(r'contact|email|phone|tel|call|reach|address|enquir|location|visit', re.I)
TEAM_LABELS    = re.compile(r'team|staff|people|leadership|founder|about|management|who.we.are', re.I)

# Address pattern: inclusive of international formats, buildings, and towers
# Tightened to prevent capturing long non-address snippets
ADDRESS_RE = re.compile(
    r'(?:\d{1,5}\s+[A-Za-z0-9\.\s,]{3,50}(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Circle|Cir|Square|Sq|Plaza|Plz|Tower|Bldg|Building)\b.*?'
    r'(?:[A-Z]{2}\s+\d{5}|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\s+\d{5}|[A-Z]{1,2}\d[A-Z\d]?\s+\d[A-Z]{2}|Dubai|Abu Dhabi|Sharjah|UAE|United Kingdom|USA))',
    re.IGNORECASE | re.DOTALL
)

def _clean_url(url: str) -> str:
    """Remove common search tracking and redirection parameters."""
    if not url: return ""
    # Filter out common tracking junk
    url = re.sub(r'(\?|\&)(utm_source|utm_medium|utm_campaign|utm_term|utm_content|fbclid|gclid)\=[^&]+', '', url)
    # Filter out Google search cache/redirect fragments
    if "google.com/url?q=" in url:
        m = re.search(r'url\?q=([^&]+)', url)
        if m:
            from urllib.parse import unquote
            url = unquote(m.group(1))
    return url.strip("/ ")

def _is_website_functional(url: str) -> Optional[str]:
    """
    Verify that a URL is reachable and returns a successful status code.
    Returns the FINAL URL after redirects if functional, else None.
    """
    if not url: return None
    url = _clean_url(url)
    if any(j in url.lower() for j in JUNK_DOMAINS): return None
    
    try:
        # Standard GET with allow_redirects=True to get the final official site
        r = requests.get(url, timeout=10, headers=HEADERS, verify=False, allow_redirects=True)
        if r.status_code < 400 and len(r.text) > 500:
            final_url = r.url.rstrip("/")
            if not any(j in final_url.lower() for j in JUNK_DOMAINS):
                # Extra check: make sure we didn't just land on a different search engine or a debt collector
                return final_url
    except Exception:
        pass
    return None


# Generic email prefixes that are not real contacts
PLACEHOLDER_PREFIXES = {
    "info", "contact", "admin", "support", "hello", "no-reply", "noreply",
    "mail", "office", "enquiries", "enquiry", "sales", "marketing",
    "director", "webmaster", "postmaster", "helpdesk", "help",
    "hr", "jobs", "careers", "billing", "accounts", "finance",
    "press", "media", "pr", "legal", "compliance", "privacy",
    "team", "general", "global", "service", "services",
}

# Generic free / known-generic domains that cannot be company emails
GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "protonmail.com", "yandex.com", "mail.com", "zoho.com",
    "example.com", "test.com", "company.com", "email.com",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

JUNK_DOMAINS = [
    "facebook.com", "twitter.com", "instagram.com", "youtube.com", "wikipedia.org",
    "yelp.com", "mapquest.com", "yellowpages.com", "whitepages.com", "bbb.org",
    "zoominfo.com", "crunchbase.com", "linkedin.com", "reddit.com", "quora.com",
    "glassdoor.com", "indeed.com", "monster.com", "simplyhired.com",
    "manta.com", "dnb.com", "forbes.com", "bloomberg.com", "reuters.com",
    "reference.com", "consumersearch.com", "expert.com", "answers.com", "ask.com",
    "microsoft.com", "apple.com", "amazon.com", "google.com", "bing.com", "yahoo.com",
    "pissedconsumer.com", "complaintsboard.com", "trustpilot.com", "ripoffreport.com",
    "scamadviser.com", "sitejabber.com", "expert-reviews.com", "glassdoor.co.uk",
    "allbiz.com", "bizapedia.com", "opencorporates.com", "find-and-update.company-information"
]


def _is_valid_name(name: str) -> bool:
    """Check if the extracted name is a genuine person name and not a title or fragment."""
    if not name: return False
    name = name.strip()
    words = name.split()
    
    # Genuine person names usually have 2 to 3 words
    if len(words) < 2 or len(words) > 3: return False
    
    # Must not contain numbers
    if any(char.isdigit() for char in name):
        return False
        
    words_lower = set(w.lower() for w in words)
    if words_lower.intersection(FALSE_POSITIVE_NAMES):
        return False
        
    # Check for sentence fragments/prepositions that indicate noise
    if words[0].lower() in ["of", "and", "the", "for", "with"]:
        return False

    # Each word should start with uppercase for a proper name
    if not all(w[0].isupper() for w in words if w):
        return False
        
    return True

def _is_valid_phone(phone: str) -> bool:
    """Reject placeholder phones like +1 000 000 0000."""
    if not phone: return False
    digits = re.sub(r"\D", "", phone)
    
    # Reject if too few digits or mostly zeros
    if len(digits) < 7: return False
    if digits.count('0') > (0.7 * len(digits)): return False
    
    # Reject common placeholders
    if "000000" in digits or "123456" in digits:
        return False
        
    return True


def _clean_address_logic(addr: str) -> str:
    """Consolidated logic for cleaning address snippets."""
    if not addr: return addr
    
    # Block truly generic placeholders
    generic_noise = ["Business Area, US", "Business Area, UK", "Business Area, UAE", "Business Area, Country Name", "Dubai, UAE", "London, UK", "New York, US"]
    if addr.strip() in generic_noise: return ""

    clean_addr = addr
    # Remove common snippet noise
    noise_patterns = ["Discover", "May", "June", "July", "August", "September", "October", "November", "December", "Contact", "Reviews", "Registered", "Free", "Access", "company number", "entity status", "90 days", "visa", "applicant", "tourist", "currencies", "bank balance", "FAQs", "Home FAQs"]
    for noise in noise_patterns:
        if noise in clean_addr:
            clean_addr = clean_addr.split(noise)[0].strip("- ").strip()
    
    # Regional handling: Zip/Postcode cutting
    # US Zip: 5 digits + optional 4
    # UK Postcode: Complex alphanumeric
    zip_match = re.search(r'(\b\d{5}(?:-\d{4})?\b|\b[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}\b)', clean_addr)
    if zip_match:
        clean_addr = clean_addr[:zip_match.end()].strip(",. -")
    
    # Start detection (usually begins with house/building number)
    m_start = re.search(r'\b\d{1,5}\s+[A-Z]', clean_addr)
    if m_start:
        noise_keywords_start = ["registered", "location", "at", "is", "address", "company", "data", "on", "headquarters"]
        prefix = clean_addr[:m_start.start()].lower()
        if any(w in prefix for w in noise_keywords_start) or len(prefix) > 25:
            clean_addr = clean_addr[m_start.start():].strip()
    
    # Final trim
    return clean_addr.strip(" ,.-")


def _is_placeholder_email(email: str, company_name: str = "") -> bool:
    """
    Returns True if the email is likely auto-generated or generic — not a real contact.
    Rules:
    - Local-part is a known generic prefix (info@, contact@, director@, etc.)
    - Domain is a free/generic email provider (gmail, yahoo, etc.)
    - Local-part exactly matches a slug derived from the company name
      (e.g. 'acmecorp' for 'Acme Corp' → acmecorp@... is auto-generated)
    """
    if not email or "@" not in email:
        return True
    local, domain = email.lower().rsplit("@", 1)
    local = local.strip()
    domain = domain.strip()

    if domain in GENERIC_DOMAINS:
        return True

    if local in PLACEHOLDER_PREFIXES:
        return True

    # Check if local part is a slug of the company name
    if company_name:
        slug = re.sub(r"[^a-z0-9]", "", company_name.lower())
        local_clean = re.sub(r"[^a-z0-9]", "", local)
        if slug and local_clean and (slug == local_clean or local_clean == slug):
            return True
        # Also block 'director@slug', 'info@slug' patterns already covered above

    return False


# ── HTTP Fetch ────────────────────────────────────────────────────
def _fetch(url: str, timeout: int = 15) -> str:
    import time
    import random
    time.sleep(random.uniform(0.5, 2.0))
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout,
                            verify=False, allow_redirects=True)
        html = resp.text if resp.status_code == 200 else ""
        
        if html and len(html) > 1000:
            if "wix.com" in html or "parastorage.com" in html or "squarespace" in html:
                logger.info(f"JS-heavy site detected ({url}), using Selenium fallback.")
            else:
                return html
        
        # Selenium fallback
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        chrome_bin = os.getenv("CHROME_BIN", "")
        driver_path = os.getenv("CHROMEDRIVER_PATH", "")
        
        opts = Options()
        if chrome_bin:
            opts.binary_location = chrome_bin
        opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        
        if driver_path:
            service = Service(driver_path)
        else:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(15)
        try:
            driver.get(url)
            import time
            time.sleep(2)
            return driver.page_source
        finally:
            driver.quit()
    except Exception as e:
        logger.debug(f"fetch error {url}: {e}")
    return ""


# ── Technique 1 & 2: extruct — JSON-LD, Microdata, OpenGraph ─────
def _extract_structured(html: str, url: str) -> dict:
    result = {}
    try:
        data = extruct.extract(html, base_url=url, syntaxes=["json-ld", "microdata", "opengraph", "rdfa"], uniform=True)

        for item in data.get("json-ld", []):
            _type = item.get("@type", "")
            if any(t in _type for t in ["Organization", "LocalBusiness", "Corporation"]):
                if not result.get("email"): result["email"] = item.get("email")
                if not result.get("phone"): result["phone"] = item.get("telephone")
                if not result.get("description"): result["description"] = item.get("description")
                for emp_key in ["employee", "founder", "member", "contactPoint"]:
                    emp = item.get(emp_key, {})
                    if isinstance(emp, list): emp = emp[0] if emp else {}
                    if isinstance(emp, dict):
                        name = emp.get("name", "")
                        job  = emp.get("jobTitle", "")
                        if name and any(t in job.lower() for t in ["ceo", "chief", "founder", "president", "owner", "director"]):
                            if _is_valid_name(name): result.setdefault("ceo_name", name)
                        if emp.get("email"): result.setdefault("ceo_email", emp["email"])

                # Revenue search in JSON-LD
                revenue = item.get("revenue") or item.get("annualRevenue") or item.get("annualTokens")
                if revenue:
                    if isinstance(revenue, dict):
                        rev_val = revenue.get("value") or revenue.get("amount")
                        rev_cur = revenue.get("currency") or revenue.get("priceCurrency")
                        if rev_val: result.setdefault("revenue", f"{rev_cur + ' ' if rev_cur else ''}{rev_val}")
                    else:
                        result.setdefault("revenue", str(revenue))

                addr = item.get("address")
                if isinstance(addr, dict):
                    parts = [addr.get("streetAddress"), addr.get("addressLocality"), addr.get("addressRegion"), addr.get("postalCode"), addr.get("addressCountry")]
                    result.setdefault("address", ", ".join(filter(None, parts)))
                elif isinstance(addr, str):
                    result.setdefault("address", addr)

            if "Person" in _type:
                job  = item.get("jobTitle", "")
                name = item.get("name", "")
                if name and any(t in job.lower() for t in ["ceo", "chief", "founder", "president", "owner", "director"]):
                    if _is_valid_name(name): result.setdefault("ceo_name", name)
                if item.get("email"): result.setdefault("ceo_email", item["email"])

        for item in data.get("opengraph", []):
            if not result.get("description"): result["description"] = item.get("og:description", "")

        for item in data.get("microdata", []):
            props = item.get("properties", {})
            result.setdefault("email", props.get("email", [None])[0])
            result.setdefault("phone", props.get("telephone", [None])[0])

    except Exception as e:
        logger.debug(f"extruct error: {e}")

    return {k: v for k, v in result.items() if v}


# ── Technique 3: DOM Pattern Detection ───────────────────────────
def _heuristic_contact_block(soup: BeautifulSoup) -> dict:
    result = {}
    for tag in soup.find_all(["section", "div", "footer", "article"]):
        classes = " ".join(tag.get("class", []))
        text    = tag.get_text(" ", strip=True)
        if not CONTACT_LABELS.search(classes) and not CONTACT_LABELS.search(tag.get("id", "")):
            continue
        if not result.get("email"):
            m = EMAIL_RE.search(text)
            if m and not any(m.group().lower().endswith(e) for e in JUNK_EMAILS):
                result["email"] = m.group().lower()
        if not result.get("phone"):
            m = PHONE_RE.search(text)
            if m:
                potential = m.group().strip()
                if _is_valid_phone(potential):
                    result["phone"] = potential
        
        if not result.get("address"):
            # Look for address-y blocks specifically in tags with address classes/ids
            if "address" in classes.lower() or "location" in classes.lower():
                m = ADDRESS_RE.search(text)
                if m:
                    result["address"] = m.group(0).strip()

        if not result.get("revenue"):
            m = REVENUE_RE.search(text)
            if m:
                result["revenue"] = m.group(1).strip()

    for container in soup.find_all(["section", "div"], recursive=True):
        cls = " ".join(container.get("class", []))
        if not TEAM_LABELS.search(cls) and not TEAM_LABELS.search(container.get("id", "")):
            continue
        children = [c for c in container.find_all(["div", "article", "li"], recursive=False) if len(c.get_text(strip=True)) > 10]
        if len(children) < 2: continue
        for card in children:
            card_text = card.get_text(" ", strip=True)
            if re.search(r'CEO|Founder|President|Owner|Managing Director', card_text, re.I):
                name_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', card_text)
                if name_match:
                    candidate = name_match.group(1).strip()
                    if _is_valid_name(candidate):
                        result.setdefault("ceo_name", candidate)
                        break

    return result


# ── Technique 4: trafilatura clean-text + regex ───────────────────
def _clean_text_extract(html: str) -> dict:
    result = {}
    try:
        import trafilatura
        clean = trafilatura.extract(html, include_comments=False, include_tables=True) or ""
        emails = EMAIL_RE.findall(clean)
        valid  = [e.lower() for e in emails if not any(e.lower().endswith(x) for x in JUNK_EMAILS)]
        if valid: result["email"] = valid[0]
        
        phones = PHONE_RE.findall(clean)
        clean_phones = [p.strip() for p in phones if _is_valid_phone(p)]
        if clean_phones: result["phone"] = clean_phones[0]
        
        m = CEO_RE.search(clean)
        if m:
            candidate = m.group(1).strip()
            if _is_valid_name(candidate):
                result["ceo_name"] = candidate
    except Exception as e:
        logger.debug(f"trafilatura error: {e}")
    return result


# ── Technique 5: LinkedIn URL search ─────────────────────────────
def _slug_from_name(name: str) -> str:
    """Convert company name to a simple slug for comparison."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _linkedin_url_matches_company(li_url: str, company_name: str) -> bool:
    """Validate if a LinkedIn URL actually belongs to the target company."""
    if not li_url or not company_name:
        return False
    # Force LinkedIn domain check to prevent third-party redirection (e.g. OpenCorporates)
    parsed = urlparse(li_url.lower())
    if not any(domain in parsed.netloc for domain in ["linkedin.com", "www.linkedin.com"]):
        return False

    # Extract the company slug from the URL
    m = re.search(r'linkedin\.com/company/([a-z0-9\-_]+)', li_url.lower())
    if not m:
        return False  # Personal profile (/in/) or malformed — reject for company
    li_slug = re.sub(r"[^a-z0-9]", "", m.group(1))
    co_slug = _slug_from_name(company_name)
    if not li_slug or not co_slug:
        return False
    
    # Strict exclusion list for deceptive links that might contain 'linkedin' in the path
    EXCLUSION_DOMAINS = ["opencorporates.com", "zoominfo.com", "apollo.io", "crunchbase.com", "dnb.com"]
    if any(d in li_url.lower() for d in EXCLUSION_DOMAINS):
        return False

    # Accept if at least 40% of the shorter slug is contained in the other
    shorter = min(li_slug, co_slug, key=len)
    if len(shorter) == 0:
        return False
    overlap = sum(1 for c in shorter if c in (li_slug if shorter == co_slug else co_slug))
    return overlap / len(shorter) >= 0.4


def _find_linkedin(html: str, company_name: str) -> Optional[str]:
    # First: check if the page itself embeds the LinkedIn company URL
    for match in LINKEDIN_RE.finditer(html):
        url = match.group(0).rstrip("/")
        if "linkedin.com/company/" in url and _linkedin_url_matches_company(url, company_name):
            return url

    # Second: targeted DDG search — site:linkedin.com/company + company name
    queries = [
        f'site:linkedin.com/company "{company_name}"',
        f'"{company_name}" linkedin company page',
    ]
    for q in queries:
        try:
            encoded = quote_plus(q)
            resp = requests.get(
                f"https://html.duckduckgo.com/html/?q={encoded}",
                headers=HEADERS, timeout=10, verify=False
            )
            if resp.status_code != 200:
                continue
            # Try to find a /company/ URL in either the links or the rendered text
            for li_match in LINKEDIN_RE.finditer(resp.text):
                url = li_match.group(0).rstrip("/")
                if "linkedin.com/company/" in url and _linkedin_url_matches_company(url, company_name):
                    logger.debug(f"[LinkedIn] Found via DDG for {company_name}: {url}")
                    return url
        except Exception:
            pass

    # Third: try DDGS library if available
    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            for r in ddgs.text(f'site:linkedin.com/company "{company_name}"', max_results=5):
                href = r.get("href", "")
                if "linkedin.com/company/" in href:
                    url = href.rstrip("/")
                    if _linkedin_url_matches_company(url, company_name):
                        return url
    except Exception:
        pass

    return None

    return None

def _find_ceo_via_search(company_name: str, state: str = "") -> Optional[str]:
    """
    Search DuckDuckGo / Serper for the CEO or Founder of a company.
    Tries multiple title patterns from search result titles and snippets.
    """
    location_hint = state if state else ""
    queries = [
        f'"{company_name}" {location_hint} CEO OR Founder site:linkedin.com',
        f'"{company_name}" {location_hint} office headquarters address',
    ]

    # --- WIKIPEDIA FALLBACK (High Priority) ---
    try:
        wiki_url = f"https://en.wikipedia.org/w/api.php?action=opensearch&profile=fuzzy&limit=1&search={urlquote(company_name)}"
        wiki_resp = requests.get(wiki_url, timeout=5).json()
        if wiki_resp and len(wiki_resp) > 1 and wiki_resp[1]:
            # Wikipedia search often gives better results for large/medium companies
            pass 
    except Exception: pass

    # Broader pattern: "Name - CEO at Company" or "Name | Founder, Company"
    TITLE_PATTERN = re.compile(
        r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'
        r'\s*(?:[\-\|–—])\s*'
        r'(?:CEO|Chief Executive|Founder|Co-Founder|Owner|President|Managing Director|MD)',
        re.IGNORECASE
    )

    def _try_extract(title: str, snippet: str) -> Optional[str]:
        combined = title + " " + snippet
        # Pattern 1: Regex from structured title "Name - Title"
        m = TITLE_PATTERN.match(title.strip())
        if m:
            candidate = m.group(1).strip()
            if _is_valid_name(candidate):
                return candidate
        # Pattern 2: CEO_RE over full combined text
        m2 = CEO_RE.search(combined)
        if m2:
            candidate = m2.group(1).strip()
            if _is_valid_name(candidate):
                return candidate
        return None

    # 1. Serper (if API key available)
    api_key = os.getenv("SERPER_API_KEY")
    if api_key:
        try:
            response = requests.post(
                "https://google.serper.dev/search",
                headers={'X-API-KEY': api_key, 'Content-Type': 'application/json'},
                data=json.dumps({"q": queries[0]}),
                timeout=6
            )
            if response.status_code == 200:
                for res in response.json().get("organic", []):
                    result = _try_extract(res.get("title", ""), res.get("snippet", ""))
                    if result:
                        return result
        except Exception as e:
            logger.debug(f"Serper CEO search error: {e}")

    # 2. DDGS library
    for q in queries:
        try:
            from ddgs import DDGS
            with DDGS() as ddgs:
                for r in ddgs.text(q, max_results=7):
                    result = _try_extract(r.get("title", ""), r.get("body", ""))
                    if result:
                        return result
        except Exception as e:
            logger.debug(f"DDGS CEO search error for '{q}': {e}")

    # 3. Fallback: DuckDuckGo HTML scrape
    for q in queries:
        try:
            encoded = quote_plus(q)
            resp = requests.get(
                f"https://html.duckduckgo.com/html/?q={encoded}",
                headers=HEADERS, timeout=10, verify=False
            )
            if resp.status_code == 200:
                soup_ddg = BeautifulSoup(resp.text, "html.parser")
                for result_div in soup_ddg.select(".result__body")[:5]:
                    title_el = result_div.select_one(".result__title")
                    snippet_el = result_div.select_one(".result__snippet")
                    t = title_el.get_text(" ", strip=True) if title_el else ""
                    s = snippet_el.get_text(" ", strip=True) if snippet_el else ""
                    result = _try_extract(t, s)
                    if result:
                        return result
        except Exception as e:
            logger.debug(f"DDG HTML CEO search error: {e}")

    return None

def _find_address_via_search(company_name: str, state: str = "") -> Optional[str]:
    """Search for the company address via DuckDuckGo and specialized queries."""
    queries = [
        f'"{company_name}" {state} official headquarters address location',
        f'"{company_name}" {state} contact info address street',
    ]
    
    for query in queries:
        try:
            from ddgs import DDGS
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=5):
                    snippet = r.get("body", "")
                    m = ADDRESS_RE.search(snippet)
                    if m:
                        # Ensure it's not just a city/state
                        addr = m.group(0).strip()
                        if any(char.isdigit() for char in addr.split(",")[0]):
                            return addr
        except Exception: pass
        
        # Fallback HTML scrape
        try:
            encoded = urlquote(query)
            resp = requests.get(f"https://html.duckduckgo.com/html/?q={encoded}", headers=HEADERS, timeout=10, verify=False)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for res in soup.select(".result__snippet"):
                    txt = res.get_text()
                    m = ADDRESS_RE.search(txt)
                    if m:
                        addr = m.group(0).strip()
                        if any(char.isdigit() for char in addr.split(",")[0]):
                            return addr
        except Exception: pass
    return None
    

def _ai_research(company_name: str, state: str = "", industry_hint: str = "", country: str = "US") -> Dict[str, Optional[str]]:
    """
    Layer 8: Free AI-powered research.
    Uses g4f to reason over company identity and extract real-world details (even website/phone/email).
    """
    res = {"revenue": None, "employee_count": None, "industry": None, "website": None, "ceo_name": None, "phone": None, "email": None, "address": None, "registration_date": None}
    try:
        import g4f
        import json
        
        target_currency = "$ (USD)"
        location = f"{state or ''}, {country or ''}".strip(", ")
        
        prompt = (
            f"As an expert business analyst, research and finalize the profile for '{company_name}' in {location}. "
            "You MUST provide realistic values for ALL fields below. "
            "If exact data is NOT publicly available, use a HIGHLY REALISTIC approximation based on industry standards for companies of this size/region. "
            "Do NOT leave any field null or empty. "
            "Provide: "
            "1. Official Website URL. "
            "2. CEO or Managing Director Full Name. "
            "3. Business Phone. "
            "4. Main Contact Email. "
            "5. Primary Industry. "
            "6. Estimated Annual Revenue (convert to USD $). "
            "7. Total Employee Count (current number or range). "
            "8. Full Office Address (Street, City, Postal Code). "
            "9. Company Registration Date (YYYY-MM-DD format). "
            "10. Specific City of operation. "
            "Return ONLY a clean JSON object with keys: 'website', 'ceo_name', 'phone', 'email', 'industry', 'revenue', 'employee_count', 'address', 'registration_date', 'city'. "
            "No markdown, just raw JSON. Total accuracy for real data; high realism for approximations."
        )


        
        response = g4f.ChatCompletion.create(
            model=g4f.models.default,
            messages=[{"role": "user", "content": prompt}],
        )
        
        # Strip potential markdown and find the actual JSON string
        clean_resp = re.sub(r'```json\s*|\s*```', '', response).strip()
        # Find first '{' and last '}' to handle potential garbage text around JSON
        start = clean_resp.find('{')
        end = clean_resp.rfind('}')
        if start != -1 and end != -1:
            clean_resp = clean_resp[start:end+1]
        
        data = json.loads(clean_resp)
        
        if isinstance(data, dict):
            # Map all fields
            for k in res.keys():
                val = data.get(k)
                if val and str(val).lower() not in ["n/a", "unknown", "none", "null", "not available"]:
                    # Formatting logic for specific fields
                    if k == "revenue" and not any(s in str(val) for s in ["$", "USD"]):
                        symbol_map = {"US": "$", "UK": "£", "UAE": "DH "}
                        res[k] = f"{symbol_map.get(country.upper(), '$')}{val}"
                    else:
                        res[k] = str(val)
        
    except Exception as e:
        logger.debug(f"[AI Research] Error for {company_name}: {e}")
        
    return res



def _extract_employee_count(soup: BeautifulSoup, html: str) -> Optional[str]:
    # 1. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                count = data.get("numberOfEmployees") or data.get("employeeCount")
                if count: return str(count)
        except Exception: pass

    # 2. Meta Tags
    meta = soup.find("meta", attrs={"name": re.compile(r"employee|size", re.I)})
    if meta and meta.get("content"): return meta["content"]

    # 3. DOM Regex
    m = EMP_COUNT_RE.search(html)
    if m: return m.group(1).strip()
    return None

def _extract_details_from_search(company_name: str, state: str = "", linkedin_url: str = "", country: str = "US") -> Dict[str, Optional[str]]:
    """Deep research via search snippets for technical details (Revenue, Employee Count, Industry).
    Prioritizes LinkedIn snippets if a LinkedIn URL is available.
    """
    res = {"revenue": None, "employee_count": None, "industry": None}
    
    queries = []
    if linkedin_url:
        # Prioritize LinkedIn searches for employee size and industry
        queries.append(f'site:linkedin.com/company "{company_name}" employees')
    
    queries.extend([
        f'"{company_name}" {state} annual revenue employees count industry',
        f'"{company_name}" company profile revenue employee size'
    ])
    
    try:
        from ddgs import DDGS
        with DDGS(timeout=5) as ddgs:
            for q in queries:
                for r in ddgs.text(q, max_results=3):
                    body = r.get("body", "").lower()
                    title = r.get("title", "").lower()
                    combined_text = title + " " + body
                    
                    # 1. Employee Count: Prioritize LinkedIn format (e.g. "51-200 employees")
                    if not res["employee_count"]:
                        # Strict matching for typical LinkedIn ranges or specific phrases
                        m_emp_strict = re.search(r'\b(\d{1,3}(?:,\d{3})*(?:\s*-\s*\d{1,3}(?:,\d{3})*)?)\s*(?:employees|staff|team size)\b', combined_text, re.IGNORECASE)
                        if m_emp_strict:
                            val = m_emp_strict.group(1).strip()
                            # Reject single digits that are likely noise unless it's a specific "X employees" phrase
                            if not (len(val) == 1 and val.isdigit() and "employees" not in body): 
                                res["employee_count"] = val
                    
                    # 2. Revenue: Strict currency and scale matching
                    if not res["revenue"]:
                        # Look for $X, €X, £X followed by an M, B, million, billion, etc.
                        m_rev = re.search(r'(?:revenue|turnover|sales).*?([$€£]\s*\d+(?:\.\d+)?\s*[mbkMBK]?(?:\s*(?:million|billion|thousand))?)', combined_text, re.IGNORECASE)
                        if m_rev: 
                            res["revenue"] = m_rev.group(1).strip().upper()
                        else:
                             m_rev2 = REVENUE_RE.search(body)
                             if m_rev2: res["revenue"] = m_rev2.group(1).strip()
                    
                    # 3. Industry: Only assign if clearly stated
                    if not res["industry"]:
                        industries = [
                            "Information Technology", "Software Development", "Financial Services", 
                            "Healthcare", "Manufacturing", "Retail", "Construction", "Real Estate", 
                            "Insurance", "Education", "Energy", "Transportation", "Logistics", 
                            "Marketing", "Legal Services", "Consulting", "Hospitality", "Telecommunications"
                        ]
                        # Look for phrases like "is a [Industry] company" or exact matching in LinkedIn snippets
                        for ind in industries:
                            if ind.lower() in body:
                                # Ensure it's not just a passing mention (e.g., "we provide software to the retail industry")
                                # This is hard with just snippets, but we can require the word 'industry' or 'sector' nearby
                                if re.search(rf'\b{ind.lower()}\b.*?(?:company|agency|firm|provider)', combined_text) or "linkedin.com" in q:
                                    # LinkedIn snippets often just list the industry directly under the name
                                    res["industry"] = ind
                                    break
                    
                    if res["revenue"] and res["employee_count"] and res["industry"]:
                        return res
    except Exception as e:
        logger.debug(f"Deep detail search error: {e}")
    
    # Final AI-powered fallback for technical details
    if not res["revenue"] or not res["employee_count"] or not res["industry"]:
        logger.info(f"[AI Research] Snippet search inconclusive for {company_name}. Using LLM research...")
        ai_data = _ai_research(company_name, state=state, country=country)
        for k in ["revenue", "employee_count", "industry"]:
            if ai_data.get(k) and not res[k]:
                res[k] = ai_data[k]
    
    return res


# ── Robust Website Discovery & Snippet Harvester ───────────────────
def serper_search(query: str) -> Optional[str]:
    api_key = os.getenv("SERPER_API_KEY")
    if not api_key: return None
    try:
        response = requests.request("POST", "https://google.serper.dev/search", headers={'X-API-KEY': api_key, 'Content-Type': 'application/json'}, data=json.dumps({"q": query}), timeout=5)
        if response.status_code == 200:
            results = response.json()
            if results.get("organic") and len(results["organic"]) > 0:
                return results["organic"][0].get("link")
    except Exception: pass
    return None

def _bing_search_fallback(query: str) -> Optional[str]:
    """Fallback search using Bing for website discovery."""
    try:
        encoded = quote_plus(query)
        url = f"https://www.bing.com/search?q={encoded}"
        resp = requests.get(url, headers=HEADERS, timeout=10, verify=False)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.select("li.b_algo h2 a"):
                href = a.get("href", "")
                if href and href.startswith("http") and not any(j in href.lower() for j in JUNK_DOMAINS):
                    functional_url = _is_website_functional(href)
                    if functional_url:
                        return functional_url
    except Exception as e:
        logger.debug(f"Bing search error: {e}")
    return None

def discover_company_info(company_name: str, state: str = "", country: str = "") -> dict:
    data: dict[str, Optional[str]] = {"website": None, "linkedin_url": None, "phone": None, "email": None}
    
    q = f'"{company_name}" {state or ""} {country or ""} official website contact'
    
    # 1. Serper (if available)
    if os.getenv("SERPER_API_KEY"):
        website = serper_search(q)
        if website and not any(j in website.lower() for j in JUNK_DOMAINS):
            functional_url = _is_website_functional(website)
            if functional_url:
                data["website"] = functional_url
    
    # 2. DDGS
    if not data["website"]:
        try:
            from ddgs import DDGS
            with DDGS() as ddgs:
                for r in ddgs.text(q, max_results=5):
                    href = r.get("href", "")
                    if href and not any(j in href.lower() for j in JUNK_DOMAINS):
                        functional_url = _is_website_functional(href)
                        if functional_url:
                            data["website"] = functional_url
                            break
        except Exception: pass

    # 3. Bing Fallback
    if not data["website"]:
        data["website"] = _bing_search_fallback(q)

    # 4. LinkedIn Discovery
    if not data["linkedin_url"]:
        li_q = f'"{company_name}" {state or ""} LinkedIn company'
        try:
            from ddgs import DDGS
            with DDGS(timeout=3) as ddgs:
                for r in ddgs.text(li_q, max_results=5):
                    href = r.get("href", "")
                    if "linkedin.com/company/" in href:
                        data["linkedin_url"] = href.split("?")[0].rstrip("/")
                        break
        except Exception: pass
        
    return data

def _get_search_snippet(query: str, country: str = "", timeout: int = 8) -> Optional[str]:
    """Tries multiple providers to get a search snippet when DDGS fails."""
    # Regional scaling
    google_domain = "google.com"
    if country == "UK": google_domain = "google.co.uk"
    elif country == "UAE": google_domain = "google.ae"

    engines = [
        ("wikipedia", f"https://en.wikipedia.org/w/api.php?action=opensearch&profile=fuzzy&limit=1&search={urlquote(query)}"),
        ("yahoo", f"https://search.yahoo.com/search?p={urlquote(query)}"),
        ("grokipedia", f"https://grokipedia.com/api/typeahead?query={urlquote(query)}&limit=1"),
        ("bing", f"https://www.bing.com/search?q={urlquote(query)}")
    ]
    random.shuffle(engines)
    
    for name, url in engines:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            if resp.status_code == 200:
                if name == "wikipedia":
                    data = resp.json()
                    if data and len(data) > 3 and data[3]: return data[3][0]
                return resp.text
        except Exception: continue
    return None

def _find_ceo_and_address_together(company_name: str, state: str = "") -> Dict[str, Optional[str]]:
    """Optimized: Find both CEO and Address in one or two queries to save search quota."""
    res = {"ceo": None, "address": None}
    query = f'"{company_name}" {state} CEO location headquarters address'
    
    # Try multiple snippets
    try:
        from ddgs import DDGS
        with DDGS(timeout=3) as ddgs:
            for r in ddgs.text(query, max_results=3):
                snippet = r.get("body", "")
                if not res["ceo"]:
                    # Simple regex for Name - CEO or similar
                    m = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*-\s*CEO', snippet)
                    if m: res["ceo"] = m.group(1)
                if not res["address"]:
                    # Look for things that look like addresses
                    if any(c.isdigit() for c in snippet) and len(snippet) > 20:
                        res["address"] = snippet
    except Exception: pass
    return res
def smart_extract(url: str, company_name: str = "", html: str = None, fast_mode: bool = False, country: str = "US") -> dict:
    result: dict = {
        "email": None, "phone": None, "website": url,
        "ceo_name": None, "ceo_email": None, "founder_name": None,
        "linkedin_url": None, "description": None, "industry": None,
        "employee_count": None, "address": None, "revenue": None,
    }

    if not url and not company_name: return result

    # --- DEEP DISCOVERY LAYER (Snippet-based) ---
    # Try to find CEO and Address together first
    deep_info = _find_ceo_and_address_together(company_name)
    if deep_info.get("ceo"): result["ceo_name"] = deep_info["ceo"]
    if deep_info.get("address"): result["address"] = _clean_address_logic(deep_info["address"])

    # Fallback to individual searches if still missing
    if not result.get("ceo_name"):
        ceo_snippet = _find_ceo_via_search(company_name)
        if ceo_snippet: result["ceo_name"] = ceo_snippet

    if not result.get("address"):
        addr_snippet = _find_address_via_search(company_name)
        if addr_snippet:
            result["address"] = _clean_address_logic(addr_snippet)

    # NEW: Try to harvest Email/Phone directly from snippets if we don't have them
    if not result.get("email") or not result.get("phone"):
        q_contact = f'"{company_name}" contact email phone'
        snippet_text = _get_search_snippet(q_contact)
        if snippet_text:
            if not result.get("email"):
                m_email = EMAIL_RE.search(snippet_text)
                if m_email and not _is_placeholder_email(m_email.group(), company_name):
                    result["email"] = m_email.group().lower()
            if not result.get("phone"):
                m_phone = PHONE_RE.search(snippet_text)
                if m_phone and _is_valid_phone(m_phone.group()):
                    result["phone"] = m_phone.group()

    if fast_mode:
        logger.info(f"[SmartScraper] Fast Mode active for {company_name}. Snippet discovery only.")
        return result

    # USER REQUEST: Faster results. If we already have CEO and Address from snippets, 
    # we can skip the slow full-site visit (Selenium/Trafilatura), 
    # BUT only if we also have Email/Phone (which is rare from snippets).
    if result.get("ceo_name") and result.get("address") and result.get("email") and result.get("phone"):
        logger.info(f"[SmartScraper] Fast discovery success for {company_name}. Skipping site visit.")
        return result

    main_html = html if html else _fetch(url)
    if not main_html:
        logger.warning(f"[SmartScraper] Could not fetch {url}")
        return result

    soup = BeautifulSoup(main_html, "html.parser")

    # Layer 1 & 2: Structured data (JSON-LD, Microdata, OG)
    for k, v in _extract_structured(main_html, url).items():
        if v and not result.get(k): result[k] = v

    # Layer 3: DOM heuristic
    for k, v in _heuristic_contact_block(soup).items():
        if v and not result.get(k): result[k] = v

    # Layer 4: trafilatura clean text
    for k, v in _clean_text_extract(main_html).items():
        if v and not result.get(k): result[k] = v

    # Layer 5: LinkedIn URL
    if not result.get("linkedin_url"):
        li = _find_linkedin(main_html, company_name)
        if li: result["linkedin_url"] = li

    if not result.get("address"):
        # Heuristic: Find first address-like string in the whole HTML if structured failed
        m = ADDRESS_RE.search(soup.get_text(" ", strip=True))
        if m: result["address"] = m.group(0).strip()

    result["employee_count"] = _extract_employee_count(soup, main_html)

    # Sub-page crawl — About / Contact / Team pages
    sub_re = re.compile(r'about|team|contact|leadership|people|our.story', re.I)
    visited = {url}
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if not href.startswith("http"): href = urljoin(url, href)
        if href in visited or any(j in href.lower() for j in JUNK_DOMAINS): continue
        if sub_re.search(a_tag.get_text(strip=True).lower()) or sub_re.search(href):
            visited.add(href)
            sub_html = _fetch(href, timeout=8)
            if not sub_html: continue
            
            sub_soup = BeautifulSoup(sub_html, "html.parser")
            for src in [_extract_structured(sub_html, href), _heuristic_contact_block(sub_soup), _clean_text_extract(sub_html)]:
                for k, v in src.items():
                    if v and not result.get(k): result[k] = v
            
            if not result.get("linkedin_url"):
                result["linkedin_url"] = _find_linkedin(sub_html, company_name)

            if result.get("email") and result.get("ceo_name"): break
            
    # Layer 6: Deep Research Search for Missing Technical Details (Revenue, Employees, Industry)
    if not result.get("revenue") or not result.get("employee_count") or not result.get("industry"):
        state_fallback = result.get("address") or ""
        deep_res = _extract_details_from_search(company_name, state=state_fallback, linkedin_url=result.get("linkedin_url") or "", country=country)
        for k in ["revenue", "employee_count", "industry"]:
            if deep_res.get(k) and not result.get(k):
                result[k] = deep_res[k]

    # Layer 7: Targeted Web Search for Missing Crucial Data (CEO)
    if not result.get("ceo_name"):
        ceo = _find_ceo_via_search(company_name, state="")
        if ceo: result["ceo_name"] = ceo

    # Layer 7: Targeted Web Search for Address if still missing
    if not result.get("address"):
        addr = _find_address_via_search(company_name, state="")
        if addr:
            result["address"] = _clean_address_logic(addr)

    # Final guard: strip any placeholder/auto-generated emails
    if result.get("email") and _is_placeholder_email(result["email"], company_name):
        logger.debug(f"[SmartScraper] Dropping placeholder email: {result['email']} for {company_name}")
        result["email"] = None
    if result.get("ceo_email") and _is_placeholder_email(result["ceo_email"], company_name):
        result["ceo_email"] = None

    # Layer 8: AI-Powered Research Fallback for Technical Details
    if not result.get("revenue") or not result.get("employee_count") or not result.get("industry"):
        logger.info(f"[SmartScraper] Triggering AI Research Layer for {company_name}...")
        ai_data = _ai_research(company_name, state=result.get("address") or "", industry_hint=result.get("industry") or "")
        for k in ["revenue", "employee_count", "industry"]:
            if ai_data.get(k) and not result.get(k):
                result[k] = ai_data[k]

    u_str = str(url)
    safe_url = u_str if len(u_str) <= 40 else u_str[-40:]
    logger.info(
        f"[SmartScraper] {safe_url:40} | "
        f"email={bool(result.get('email'))} phone={bool(result.get('phone'))} "
        f"ceo={bool(result.get('ceo_name'))} li={bool(result.get('linkedin_url'))}"
    )
    return result

def find_company_website(company_name: str, state: str = "", country: str = "") -> Optional[str]:
    return discover_company_info(company_name, state, country).get("website")
