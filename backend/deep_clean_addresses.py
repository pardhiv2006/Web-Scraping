
import os
import re
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "businesses.db")

def clean_address_text(addr):
    if not addr: return addr
    
    # 1. Truncate after Zip Code
    zip_match = re.search(r'(\b\d{5}(?:-\d{4})?\b|\b[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}\b)', addr)
    if zip_match:
        addr = addr[:zip_match.end()].strip()
    
    # 2. Find the start of the address (usually a number)
    # But skip years like "2008 in..."
    m_start = re.search(r'\b\d{1,5}\s+[A-Z]', addr)
    if m_start:
        # Check if what's before it is noise
        noise_keywords = ["registered", "location", "at", "is", "address", "company", "data", "on"]
        prefix = addr[:m_start.start()].lower()
        if any(w in prefix for w in noise_keywords) or len(prefix) > 20:
            addr = addr[m_start.start():].strip()
            
    # 3. Final cleanup of noise keywords at the end if no zip match
    for noise in ["Discover", "May", "June", "July", "August", "September", "October", "November", "December", "Contact", "Reviews", "Registered", "Free", "Access", "company number"]:
        if noise in addr:
            addr = addr.split(noise)[0].strip("- ").strip()
            
    return addr.strip(" ,.-")

def run_deep_clean():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, address FROM businesses WHERE address IS NOT NULL AND address != ''")
    rows = cursor.fetchall()
    
    updated = 0
    for row_id, addr in rows:
        cleaned = clean_address_text(addr)
        if cleaned != addr:
            cursor.execute("UPDATE businesses SET address = ? WHERE id = ?", (cleaned, row_id))
            updated += 1
            
    conn.commit()
    conn.close()
    print(f"Deep clean complete. Cleaned {updated} addresses.")

if __name__ == "__main__":
    run_deep_clean()
