"""
cleanup_bad_data.py
===================
Identifies and resets 'junk' data in the business registry database.
Junk includes:
- Search snippets in address fields.
- Generic placeholders like 'ENG' for city.
- CEO names that are actually email markers.
- Templated descriptions that lack real info.
"""

import os
import sys
import sqlite3
import re

DB_PATH = 'businesses.db'
if not os.path.exists(DB_PATH):
    # Try backend directory
    DB_PATH = os.path.join(os.path.dirname(__file__), 'businesses.db')

def is_junk_address(addr: str) -> bool:
    if not addr: return False
    addr_lower = addr.lower()
    junk_keywords = [
        'trial aims', 'bhp', 'parent company', 'iso 9001', 
        'privacy & cookies', 'request a quote', 'workspace interiors',
        'document management', 'digital printing', 'entity #',
        'limited liability partnership', 'department of state',
        'obtained a listing', 'london stock exchange', 'dec 16, 2025',
        'established 1997', 'specialist contractor', 'bounces back',
        'proposed to', 'high-profile cases', 'million loss', 'ksh',
        'contact us', 'all rights reserved', 'terms of service', 'amazon\'s',
        'below is a list', 'retail locations', 'view contact profiles',
        'sic code', 'naics code', 'show more', 'popular searches',
        'global inc', 'pte ltd', 'earlier forensics', 'headquartered in washington'
    ]
    if any(kw in addr_lower for kw in junk_keywords):
        return True
    if len(addr) > 250: # Real addresses are rarely this long
        return True
    if len(addr) > 50 and not any(char.isdigit() for char in addr):
        return True
    return False

def is_junk_city(city: str) -> bool:
    if not city: return False
    city_lower = city.lower()
    placeholders = ['eng', 'sct', 'dxb', 'auh', 'local region', 'nir', 'wales', 'shj', 'as of', 'ltd', 'inc', 'corp', 'dec 16', 'unknown']
    if city_lower.strip() in placeholders: return True
    if len(city) < 2 or len(city) > 40: return True
    if len(city.split()) > 3: return True
    if re.match(r'^[A-Z0-9]{2,4}\s?[A-Z0-9]{3}$', city.upper()):
        return True
    return False

def is_junk_ceo(name: str) -> bool:
    if not name: return False
    junk = ['email address', 'contact information', 'unknown', 'pending', 'ceo', 'founder']
    if any(j in name.lower() for j in junk):
        return True
    return False

def cleanup():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, company_name, email, phone, website, ceo_name, ceo_email, founder_name, linkedin_url, industry, employee_count, description, city, revenue, address FROM businesses")
    rows = cursor.fetchall()
    
    total_reset = 0
    
    for row in rows:
        biz_id = row[0]
        updates = {}
        
        # Check Address & City
        if is_junk_address(row[14]):
            updates['address'] = None
            print(f"[RESET] Junk Address for ID {biz_id} ({row[1]})")
            
        if is_junk_city(row[12]):
            updates['city'] = None
            print(f"[RESET] Junk City for ID {biz_id} ({row[1]})")
            
        # Check CEO
        if is_junk_ceo(row[5]):
            updates['ceo_name'] = None
            print(f"[RESET] Junk CEO for ID {biz_id} ({row[1]})")
            
        # Check Revenue (Raw numbers)
        if row[13] and row[13].isdigit():
            # Standardize or reset if it's just a raw large number
            updates['revenue'] = None 
            print(f"[RESET] Non-standard Revenue for ID {biz_id} ({row[1]})")

        # Check Description (Template fallback)
        if row[11] and "is a" in row[11] and "company based in" in row[11] and len(row[11]) < 150:
            # This looks like the generic template from bulk_fix_all_blanks.py
            # Only reset if we want to force a real scrape
            pass 

        if updates:
            set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
            params = list(updates.values()) + [biz_id]
            cursor.execute(f"UPDATE businesses SET {set_clause} WHERE id = ?", params)
            total_reset += 1

    conn.commit()
    conn.close()
    print(f"Cleanup complete. {total_reset} records were partially reset.")

if __name__ == "__main__":
    cleanup()
