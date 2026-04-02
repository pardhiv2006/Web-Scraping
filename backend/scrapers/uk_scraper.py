"""
UK Business Registry Scraper.
Primary source: Companies House free REST API.
Returns ONLY real API data. NEVER generates synthetic records.
"""
import logging
import os
import requests
from typing import List, Dict

logger = logging.getLogger(__name__)

CH_BASE = "https://api.company-information.service.gov.uk"
CH_API_KEY = os.getenv("COMPANIES_HOUSE_API_KEY", "")

HEADERS_BASE = {"User-Agent": "BusinessRegistryScraper/2.0", "Accept": "application/json"}

REGION_KEYWORDS = {
    "ENG": ["London", "Manchester", "Birmingham", "Leeds", "Bristol"],
    "SCT": ["Edinburgh", "Glasgow", "Aberdeen"],
    "WLS": ["Cardiff", "Swansea", "Newport"],
    "NIR": ["Belfast", "Derry", "Lisburn"],
}

def _build_headers() -> dict:
    h = dict(HEADERS_BASE)
    if CH_API_KEY: h["Authorization"] = CH_API_KEY
    return h

def _parse_ch_item(item: dict, region: str) -> Dict:
    addr = item.get("registered_office_address") or item.get("address") or {}
    address_parts = [addr.get("address_line_1"), addr.get("address_line_2"), addr.get("locality"), addr.get("postal_code"), "United Kingdom"]
    company_number = item.get("company_number", "")
    return {
        "company_name": (item.get("company_name") or item.get("title") or "").strip(),
        "registration_number": company_number,
        "country": "UK",
        "state": region,
        "registration_date": item.get("date_of_creation") or item.get("date_of_incorporation") or None,
        "address": ", ".join(filter(None, address_parts)) or None,
        "status": (item.get("company_status") or "Active").title(),
        "source_url": f"https://find-and-update.company-information.service.gov.uk/company/{company_number}" if company_number else "",
    }

def _fetch_advanced_search(region: str, per_page: int = 25) -> List[Dict]:
    if not CH_API_KEY: return []
    try:
        resp = requests.get(f"{CH_BASE}/advanced-search/companies", params={"incorporated_from": "2025-01-01", "size": per_page, "start_index": 0}, headers=_build_headers(), timeout=15)
        if resp.status_code != 200: return []
        return [_parse_ch_item(item, region) for item in resp.json().get("items", []) if item.get("company_name") or item.get("title")]
    except Exception: return []

def _fetch_public_search(region: str, keyword: str, per_page: int = 20) -> List[Dict]:
    try:
        resp = requests.get(f"{CH_BASE}/search/companies", params={"q": keyword, "items_per_page": per_page}, headers=_build_headers(), timeout=15)
        if resp.status_code != 200: return []
        return [_parse_ch_item(item, region) for item in resp.json().get("items", []) if item.get("title")]
    except Exception: return []

def scrape_uk(states: List[str]) -> List[Dict]:
    all_records: List[Dict] = []
    for region in states:
        logger.info(f"[UK] Fetching real data for region: {region}")
        records = _fetch_advanced_search(region)
        if not records:
            keywords = REGION_KEYWORDS.get(region, [region])
            seen_numbers = set()
            for kw in keywords:
                for rec in _fetch_public_search(region, kw):
                    rn = rec.get("registration_number", "")
                    if rn and rn not in seen_numbers:
                        seen_numbers.add(rn)
                        records.append(rec)
                if records: break
        all_records.extend(records)
    return all_records
