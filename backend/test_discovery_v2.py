import sys
import os
sys.path.append(os.getcwd())

from services.smart_scraper import discover_company_info
from services.enrichment_service import enrich_business
from database import SessionLocal
from models.business import Business

def test_discovery():
    test_cases = [
        "Ridge Co",
        "Bright Consulting LLC",
        "Pardhiv Intern",
    ]
    
    for name in test_cases:
        print(f"\n--- Testing Discovery for: {name} ---")
        info = discover_company_info(name)
        print(f"Discovered: {info}")

if __name__ == "__main__":
    test_discovery()
