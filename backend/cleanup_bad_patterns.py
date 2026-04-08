import sqlite3
import re
import os

# Database Path
DB_PATH = 'businesses.db'

# Patterns that indicate a generic or bad entry
BAD_URL_PATTERNS = [
    'google.com/search',
    'bing.com/search',
    'duckduckgo.com',
    'opencorporates.com',
    'bizapedia.com',
    'find-and-update.company-information.service.gov.uk',
    'yelp.com',
    'yellowpages.com'
]

GENERIC_DESCRIPTIONS = [
    'A professional company operating in US.',
    'A professional company operating in UK.',
    'A professional company operating in UAE.',
    'A professional company.',
    'No description available.',
    'General business services.'
]

def cleanup():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("--- Starting Database Cleanup ---")

    # 1. Nullify website URLs that are search engine links
    for pattern in BAD_URL_PATTERNS:
        cursor.execute(f"UPDATE businesses SET website = NULL WHERE website LIKE '%{pattern}%'")
        print(f"Nullified websites containing: {pattern} ({cursor.rowcount} rows)")

    # 2. Nullify LinkedIn URLs pointing to OpenCorporates or Bizapedia
    for pattern in ['opencorporates.com', 'bizapedia.com']:
        cursor.execute(f"UPDATE businesses SET linkedin_url = NULL WHERE linkedin_url LIKE '%{pattern}%'")
        print(f"Nullified LinkedIn URLs containing: {pattern} ({cursor.rowcount} rows)")

    # 3. Nullify generic descriptions
    for desc in GENERIC_DESCRIPTIONS:
        cursor.execute("UPDATE businesses SET description = NULL WHERE description = ?", (desc,))
        print(f"Nullified generic description: '{desc}' ({cursor.rowcount} rows)")

    # 4. Nullify generic industry labels
    cursor.execute("UPDATE businesses SET industry = NULL WHERE industry = 'General Business Services'")
    print(f"Nullified generic industry: 'General Business Services' ({cursor.rowcount} rows)")

    # 5. Nullify emails that are search result placeholders
    for pattern in ['google.com', 'bizapedia.com', 'find-and-update']:
        cursor.execute(f"UPDATE businesses SET email = NULL WHERE email LIKE '%{pattern}%'")
        print(f"Nullified placeholder emails containing: {pattern} ({cursor.rowcount} rows)")

    conn.commit()
    print("--- Cleanup Complete ---")
    conn.close()

if __name__ == "__main__":
    cleanup()
