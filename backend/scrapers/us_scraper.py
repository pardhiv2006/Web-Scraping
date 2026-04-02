"""
US Business Registry Scraper.
Primary source: OpenCorporates API (free tier, no key required).
Returns ONLY real API data. If APIs are unavailable, returns [] — never synthetic records.
"""
import logging
import requests
from typing import List, Dict

logger = logging.getLogger(__name__)

OPENCORPORATES_BASE = "https://api.opencorporates.com/v0.4"

HEADERS = {
    "User-Agent": "BusinessRegistryScraper/2.0 (data-enrichment)",
    "Accept": "application/json",
}

def _fetch_from_opencorporates(state: str, per_page: int = 30) -> List[Dict]:
    jurisdiction = f"us_{state.lower()}"
    records: List[Dict] = []

    param_sets = [
        {"jurisdiction_code": jurisdiction, "incorporation_date": "2025-01-01:", "order": "incorporation_date", "per_page": per_page},
        {"jurisdiction_code": jurisdiction, "per_page": per_page},
    ]

    for params in param_sets:
        if records: break
        try:
            resp = requests.get(f"{OPENCORPORATES_BASE}/companies/search", params=params, timeout=15, headers=HEADERS)
            if resp.status_code == 429:
                logger.warning(f"[US/{state}] OpenCorporates rate-limited (429). Skipping.")
                return []
            if resp.status_code != 200: continue

            companies = resp.json().get("results", {}).get("companies", [])
            for item in companies:
                co = item.get("company", {})
                if not co.get("name"): continue

                addr_parts = co.get("registered_address") or {}
                address = ", ".join(filter(None, [addr_parts.get("street_address"), addr_parts.get("locality"), addr_parts.get("region"), addr_parts.get("postal_code"), "USA"])) or None

                records.append({
                    "company_name": co.get("name", "").strip(),
                    "registration_number": co.get("company_number", ""),
                    "country": "US",
                    "state": state,
                    "registration_date": co.get("incorporation_date") or None,
                    "address": address,
                    "status": (co.get("current_status") or "Active").title(),
                    "source_url": co.get("opencorporates_url") or "",
                })
        except Exception as e:
            logger.warning(f"[US/{state}] OpenCorporates API error: {e}")

    return records

def scrape_us(states: List[str]) -> List[Dict]:
    all_records: List[Dict] = []
    for state in states:
        logger.info(f"[US] Fetching real data for state: {state}")
        try:
            records = _fetch_from_opencorporates(state)
            all_records.extend(records)
        except Exception as e:
            logger.error(f"[US/{state}] Unexpected error: {e}")
    return all_records
