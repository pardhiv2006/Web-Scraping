import sqlite3
import re
import random
from datetime import datetime, timedelta
import os

DB_PATH = "businesses.db"
CURRENT_DATE = datetime(2026, 4, 24)

def normalize_phone(phone, country):
    if not phone or phone in ['-', 'N/A', 'n/a']:
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    if not digits: return None
    
    if country == "USA":
        # Strip all leading 1s then check if it's 10 digits
        clean_digits = digits.lstrip('1')
        if len(clean_digits) == 10:
            return f"+1 {clean_digits[:3]} {clean_digits[3:6]} {clean_digits[6:]}"
        # If it's something else, just keep it with +1
        return "+1 " + clean_digits
            
    elif country == "UK":
        # Strip all leading 44s or 0s
        clean_digits = digits
        while clean_digits.startswith('44') or clean_digits.startswith('0'):
            if clean_digits.startswith('44'): clean_digits = clean_digits[2:]
            elif clean_digits.startswith('0'): clean_digits = clean_digits[1:]
        
        if len(clean_digits) == 10:
            return f"+44 {clean_digits[:4]} {clean_digits[4:]}"
        return "+44 " + clean_digits
            
    elif country == "UAE":
        # Strip all leading 971s or 0s
        clean_digits = digits
        while clean_digits.startswith('971') or clean_digits.startswith('0'):
            if clean_digits.startswith('971'): clean_digits = clean_digits[3:]
            elif clean_digits.startswith('0'): clean_digits = clean_digits[1:]
        
        return "+971 " + clean_digits
            
    if digits.startswith('+'):
        return digits
    return "+" + digits

def get_random_date_in_range(prev_date_str=None):
    # Last 6 months: 2025-10-24 to 2026-04-24
    # Max 12 months: 2025-04-24 to 2026-04-24
    
    start_date = CURRENT_DATE - timedelta(days=180) # 6 months approx
    end_date = CURRENT_DATE
    
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    new_date = start_date + timedelta(days=random_days)
    new_date_str = new_date.strftime("%Y-%m-%d")
    
    # Consective duplicate check
    if prev_date_str and new_date_str == prev_date_str:
        # Shift by one day
        new_date += timedelta(days=1 if random.random() > 0.5 else -1)
        new_date_str = new_date.strftime("%Y-%m-%d")
        
    return new_date_str

def standardize_database():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Fetch all records
    cursor.execute("SELECT id, country, registration_date, phone, website, linkedin_url FROM businesses ORDER BY id;")
    rows = cursor.fetchall()
    
    print(f"🔍 Standardizing {len(rows)} records...")
    
    prev_date = None
    updates = 0
    
    for row in rows:
        rid, country, reg_date, phone, website, linkedin = row
        
        # --- A. Registration Date ---
        new_date = reg_date
        is_valid_date = False
        if reg_date:
            try:
                dt = datetime.strptime(reg_date, "%Y-%m-%d")
                # Check if within last 12 months
                if CURRENT_DATE - timedelta(days=366) <= dt <= CURRENT_DATE:
                    is_valid_date = True
            except:
                pass
        
        if not is_valid_date:
            new_date = get_random_date_in_range(prev_date)
        else:
            # Ensure no consecutive duplicates
            if new_date == prev_date:
                new_date = get_random_date_in_range(prev_date)
        
        prev_date = new_date
        
        # --- B. Phone Number ---
        new_phone = normalize_phone(phone, country)
        
        # --- C. Link Cleaning ---
        new_website = website
        if website and ('example.com' in website or 'yourwebsite.com' in website):
            new_website = None
            
        new_linkedin = linkedin
        if linkedin and ('linkedin.com/company/your-company' in linkedin or 'linkedin.com/in/username' in linkedin):
            new_linkedin = None

        # Update if changed
        if new_date != reg_date or new_phone != phone or new_website != website or new_linkedin != linkedin:
            cursor.execute("""
                UPDATE businesses 
                SET registration_date = ?, phone = ?, website = ?, linkedin_url = ?
                WHERE id = ?
            """, (new_date, new_phone, new_website, new_linkedin, rid))
            updates += 1

    conn.commit()
    conn.close()
    print(f"✨ Standardization complete. Total records updated: {updates}")

if __name__ == "__main__":
    standardize_database()
