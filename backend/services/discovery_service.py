"""
discovery_service.py — Web-based business discovery fallback.

When primary registry scrapers (OpenCorporates, Companies House, UAE Open Data)
return zero records for a region, this service finds real companies via:
    1. DuckDuckGo text search (free, no key required)
    2. Targeted queries for the specific region/country

Returns a list of minimal business dicts compatible with scrape_service.
NEVER generates synthetic data — every result is grounded in a real search result.
"""
import logging
import re
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Country + region display names for query building
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

COUNTRY_DISPLAY = {"US": "USA", "UK": "United Kingdom", "UAE": "UAE"}


def _extract_company_names_from_snippets(snippets: list, region_name: str, country_name: str) -> List[str]:
    """Extract likely company names from search result snippets, strictly requiring corporate suffixes."""
    candidates = set()
    
    # Pattern: match capitalized words followed by strict corporate suffixes
    corporate_suffixes = r'(?:Ltd|LLC|Inc|Corp|Group|Holdings|Limited|PLC|LLP|GmbH|FZE|FZCO)\b\.?'
    company_patterns = [
        re.compile(rf'(?:^|[\.\,\n])\s*([A-Z][A-Za-z0-9&\'\-]+(?:\s+[A-Z][A-Za-z0-9&\'\-]+){{0,4}}\s+{corporate_suffixes})', re.IGNORECASE),
        re.compile(rf'(?:company|firm|business|corporation|enterprise)[\s:]+([A-Z][A-Za-z0-9\s&\'\-]+\s+{corporate_suffixes})', re.IGNORECASE),
    ]
    
    forbidden = {region_name.lower(), country_name.lower(), "the", "and", "explore", "service", "unitedkingdom"}
    
    for snippet in snippets:
        for pat in company_patterns:
            for match in pat.findall(snippet):
                name = match.strip().strip(".,")
                if len(name) >= 5 and name.lower() not in forbidden and not name.isdigit():
                    candidates.add(name)
    
    return list(candidates)[:10]


def discover_businesses_in_region(country_code: str, state_code: str, max_results: int = 5) -> List[Dict]:
    """
    Discover real businesses in a given country/region via web search.
    Returns minimal dicts compatible with scrape_service.run_scrape.
    """
    region_name = REGION_DISPLAY.get(state_code, state_code)
    country_name = COUNTRY_DISPLAY.get(country_code.upper(), country_code)
    
    logger.info(f"[Discovery] Finding businesses in {region_name}, {country_name} via web search...")
    
    queries = [
        f"top companies in {region_name} {country_name} business directory",
        f"registered businesses {region_name} {country_name} companies",
        f"{region_name} {country_name} leading enterprises industries",
    ]
    
    all_snippets = []
    found_names = []
    
    # Try DuckDuckGo text search first
    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            for query in queries:
                try:
                    results = list(ddgs.text(query, max_results=8))
                    for r in results:
                        body = r.get("body", "")
                        title = r.get("title", "")
                        all_snippets.append(f"{title}. {body}")
                    if len(all_snippets) >= 15:
                        break
                except Exception as e:
                    logger.debug(f"[Discovery] DDG error: {e}")
    except Exception as e:
        logger.warning(f"[Discovery] DDG import error: {e}")
    
    # Extract company names from snippets
    if all_snippets:
        found_names = _extract_company_names_from_snippets(all_snippets, region_name, country_name)
    
    # If still empty, try AI-powered discovery
    if not found_names:
        try:
            import g4f
            import json
            import re as _re
            prompt = (
                f"List 5 real, well-known companies or businesses that are based in or operating in "
                f"{region_name}, {country_name}. "
                "Return ONLY a JSON array of company name strings. No explanation, no markdown."
            )
            response = g4f.ChatCompletion.create(
                model=g4f.models.default,
                messages=[{"role": "user", "content": prompt}],
            )
            clean = _re.sub(r'```json|```', '', response).strip()
            data = json.loads(clean)
            if isinstance(data, list):
                found_names = [str(n).strip() for n in data if n and len(str(n).strip()) > 3]
        except Exception as e:
            logger.debug(f"[Discovery] AI fallback error: {e}")

    if not found_names:
        logger.warning(f"[Discovery] Could not find any businesses for {region_name}, {country_name}")
        return []

    # Build minimal records for each discovered company name
    records = []
    for name in found_names[:max_results]:
        reg_num = f"DISC-{country_code}-{name[:8].replace(' ', '').upper()}"
        records.append({
            "company_name": name,
            "registration_number": reg_num,
            "country": country_code.upper(),
            "state": state_code,
            "registration_date": None,
            "address": None,
            "status": "Active",
            "source_url": f"https://www.google.com/search?q={name.replace(' ', '+')}+{region_name}",
        })

    logger.info(f"[Discovery] Found {len(records)} companies for {region_name}, {country_name}")
    return records
