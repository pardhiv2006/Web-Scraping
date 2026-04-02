
import os
import sys
import logging

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from services.smart_scraper import smart_extract, discover_company_info

logging.basicConfig(level=logging.INFO)

def test_company(name, state="", country="US"):
    print(f"\n--- Testing: {name} ({state}, {country}) ---")
    info = discover_company_info(name, state, country)
    website = info.get("website")
    print(f"Discovered Website: {website}")
    
    if website:
        result = smart_extract(website, company_name=name)
        print(f"Email: {result.get('email')}")
        print(f"Phone: {result.get('phone')}")
        print(f"Address: {result.get('address')}")
        print(f"CEO: {result.get('ceo_name')}")
        print(f"LinkedIn: {result.get('linkedin_url')}")
    else:
        # Try direct address search if no website
        from services.smart_scraper import _find_address_via_search
        addr = _find_address_via_search(name, state)
        print(f"Address (Search Only): {addr}")

if __name__ == "__main__":
    # Test a few companies
    test_company("Keystone Co", "CA", "US")
    test_company("Liberty Partners LLC", "US")
    test_company("Unity Industries Inc", "US")
