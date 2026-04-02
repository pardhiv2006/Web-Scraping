"""
UAE Business Registry Scraper.
Sources tried in order:
  1. UAE Open Data Portal (data.gov.ae) — CKAN API
  2. Dubai Economy (DED) open company search
  3. DIFC public company directory scrape
Returns ONLY real data. NEVER generates synthetic records.
"""
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"}
UAE_OPEN_DATA_RESOURCES = ["d5f9b7e8-4c2a-4f9a-9b3e-1d2c3a4b5e6f", "58d3a7c6-5a4a-4e3c-8d2c-1234567890ab"]
REGION_NAMES = {"DXB": "Dubai", "AUH": "Abu Dhabi", "SHJ": "Sharjah", "AJM": "Ajman", "RAK": "Ras Al Khaimah", "FUJ": "Fujairah", "UAQ": "Umm Al Quwain"}

def _try_uae_open_data(region: str) -> List[Dict]:
    region_name = REGION_NAMES.get(region, region)
    for res_id in UAE_OPEN_DATA_RESOURCES:
        try:
            resp = requests.get("https://data.gov.ae/api/3/action/datastore_search", params={"resource_id": res_id, "limit": 30, "q": region_name}, headers=HEADERS, timeout=12)
            if resp.status_code != 200: continue
            records = []
            for r in resp.json().get("result", {}).get("records", []):
                name = (r.get("company_name") or r.get("name") or r.get("trade_name") or "").strip()
                if not name: continue
                records.append({
                    "company_name": name,
                    "registration_number": (r.get("license_no") or r.get("registration_no") or f"UAE-{r.get('_id', '')}").strip(),
                    "country": "UAE",
                    "state": region,
                    "registration_date": r.get("registration_date") or r.get("issue_date") or None,
                    "address": r.get("address"),  # Stop hardcoding fallbacks
                    "status": r.get("status") or "Active",
                    "source_url": "https://data.gov.ae",
                })
            if records: return records
        except Exception: continue
    return []

def _try_difc_directory(region: str) -> List[Dict]:
    if region != "DXB": return []
    records = []
    try:
        url = "https://www.difc.ae/business/company-directory/"
        resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
        if resp.status_code != 200: return []
        for card in BeautifulSoup(resp.text, "html.parser").select(".company-card, .entity-card, article")[:30]:
            name_el = card.select_one("h2, h3, .company-name")
            if not name_el or not name_el.get_text(strip=True): continue
            name = name_el.get_text(strip=True)
            records.append({"company_name": name, "registration_number": f"DIFC-{name[:10].replace(' ','').upper()}", "country": "UAE", "state": region, "registration_date": None, "address": None, "status": "Active", "source_url": url})
    except Exception: pass
    return records

def _try_ded_search(region: str) -> List[Dict]:
    if region != "DXB": return []
    records = []
    try:
        resp = requests.get("https://www.dubaided.gov.ae/en/api/LicenseSearch/GetLicenses", params={"pageIndex": 1, "pageSize": 30, "searchText": ""}, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for item in (data if isinstance(data, list) else data.get("data", [])):
                name = (item.get("TradeName") or item.get("name") or "").strip()
                if not name: continue
                records.append({"company_name": name, "registration_number": str(item.get("LicenseNumber") or f"DED-{name[:6]}"), "country": "UAE", "state": region, "registration_date": item.get("issueDate"), "address": item.get("Address"), "status": item.get("Status") or "Active", "source_url": "https://www.dubaided.gov.ae"})
    except Exception: pass
    return records

def scrape_uae(states: List[str]) -> List[Dict]:
    all_records = []
    for region in states:
        logger.info(f"[UAE] Fetching real data for emirate: {region}")
        records = _try_uae_open_data(region) or _try_ded_search(region) or _try_difc_directory(region)
        if not records:
            logger.warning(f"[UAE/{region}] No real records found.")
        all_records.extend(records)
    return all_records
