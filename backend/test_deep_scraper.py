
import sys
import os
import json

# Add the project root to sys.path
sys.path.append(os.getcwd())

from services.smart_scraper import smart_extract

test_urls = [
    {"name": "Ridge", "url": "https://ridge.com"},
    {"name": "Bright Consulting", "url": "https://bright.consulting"},
]

print("--- Testing Deep Extraction ---")
for case in test_urls:
    print(f"\nProcessing: {case['name']} ({case['url']})...")
    data = smart_extract(case['url'], company_name=case['name'])
    print(f"Extracted Data: {json.dumps(data, indent=2)}")

print("\n--- Deep Extraction Test Complete ---")
