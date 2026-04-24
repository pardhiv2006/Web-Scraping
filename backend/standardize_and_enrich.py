import csv
import re
import os
import sys
import hashlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add backend to path to import models and database
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

from database import SessionLocal, engine
from models.business import Business

# --- Constants & Maps ---

US_STATE_MAP = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
    "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

UK_STATE_MAP = {
    "SCT": "Scotland", "SCOTLAND": "Scotland",
    "ENG": "England", "ENGLAND": "England",
    "WLS": "Wales", "WALES": "Wales",
    "NIR": "Northern Ireland", "NORTHERN IRELAND": "Northern Ireland",
}

INDUSTRY_MAP = {
    "GENERAL BUSINESS SERVICES": "Business Services",
    "INFORMATION TECHNOLOGY": "Information Technology",
    "DIVERSIFIED BUSINESS": "Business Services",
    "MANAGEMENT CONSULTING": "Consulting",
    "INVESTMENT & HOLDINGS": "Finance",
    "PROFESSIONAL SERVICES": "Business Services",
    "BUSINESS CONSULTING AND SERVICES": "Consulting",
    "TECHNOLOGY CONSULTING": "Consulting",
    "INFORMATION TECHNOLOGY SERVICES": "Information Technology",
    "TECHNOLOGY, INFORMATION AND INTERNET": "Information Technology",
    "CONSTRUCTION AND ENGINEERING": "Construction",
    "CONSTRUCTION MANAGEMENT": "Construction",
    "HEALTHCARE SOFTWARE": "Healthcare",
    "HEALTH INSURANCE": "Insurance",
}

def clean_value(v):
    if v is None: return ""
    v = str(v).strip()
    return v

def normalize_country(country):
    c = clean_value(country).upper()
    aliases = {
        "US": "USA", "USA": "USA", "UNITED STATES": "USA", "UNITED STATES OF AMERICA": "USA",
        "UK": "UK", "GB": "UK", "GREAT BRITAIN": "UK", "UNITED KINGDOM": "UK",
        "UAE": "UAE", "UNITED ARAB EMIRATES": "UAE"
    }
    return aliases.get(c, c)

def normalize_state(state, country):
    s = clean_value(state)
    if not s: return ""
    upper = s.upper()
    country = normalize_country(country)
    
    if country == "USA":
        if upper in US_STATE_MAP:
            return US_STATE_MAP[upper]
        for abbr, name in US_STATE_MAP.items():
            if upper == name.upper():
                return name
    elif country == "UK":
        if upper in UK_STATE_MAP:
            return UK_STATE_MAP[upper]
        for abbr, name in UK_STATE_MAP.items():
            if upper == name.upper():
                return name
    return s

def clean_hyperlink(value):
    if not value: return ""
    m = re.search(r'HYPERLINK\s*\(\s*"([^"]+)"', value, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return value.strip()

def normalize_website(url):
    url = clean_hyperlink(url)
    if not url: return ""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url.lower()

def normalize_linkedin(url):
    url = clean_hyperlink(url)
    if not url: return ""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url.lower()

def normalize_email(email):
    email = clean_value(email).lower()
    if not email: return ""
    if "@" not in email or "." not in email:
        return ""
    return email

def normalize_phone(phone, country):
    phone = clean_value(phone)
    if not phone: return ""
    digits = re.sub(r'[^\d+]', '', phone)
    
    country = normalize_country(country)
    if country == "USA":
        if len(digits) == 10:
            return f"+1 ({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
        elif len(digits) == 11 and digits.startswith('1'):
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
        elif digits.startswith('+1') and len(digits) == 12:
            return f"+1 ({digits[2:5]}) {digits[5:8]}-{digits[8:12]}"
    elif country == "UK":
        if digits.startswith('0') and len(digits) == 11:
            return f"+44 {digits[1:5]} {digits[5:]}"
        elif digits.startswith('44') and len(digits) == 12:
            return f"+44 {digits[2:6]} {digits[6:]}"
    return phone

def normalize_industry(ind):
    ind = clean_value(ind)
    if not ind: return ""
    upper = ind.upper()
    if upper in INDUSTRY_MAP:
        return INDUSTRY_MAP[upper]
    return ind.title()

def is_garbage_address(addr):
    if not addr: return True
    garbage_keywords = ["Home FAQs", "About us", "Contact us", "My account", "Shopping cart", "domain is for sale", "HYPERLINK", "30-day money back", "Enjoy zero percent financing"]
    for word in garbage_keywords:
        if word.lower() in addr.lower():
            return True
    if len(addr) > 200: 
        return True
    return False

def format_address(address, city, state, postal_code, country):
    components = []
    
    full_state = normalize_state(state, country)
    full_country = normalize_country(country)
    
    if address and not is_garbage_address(address):
        addr_clean = address
        # Aggressive cleanup of trailing abbreviations
        addr_clean = re.sub(r',\s*[A-Z]{2}(?:,\s*[A-Z]{2})*,\s*(?:US|USA|UK|GB|US)$', '', addr_clean, flags=re.I)
        addr_clean = re.sub(r',\s*(?:USA|UK|GB|US)(?:,\s*(?:USA|UK|GB|US))*$', '', addr_clean, flags=re.I)
        components.append(addr_clean.strip())
    
    # Don't append city if it's just a state abbr or already in address
    if city and city.upper() not in US_STATE_MAP and city.upper() not in ["US", "USA", "UK", "GB"]:
        exists = False
        for c in components:
            if city.lower() in c.lower():
                exists = True
                break
        if not exists:
            components.append(city)
            
    if full_state:
        exists = False
        for c in components:
            if full_state.lower() in c.lower():
                exists = True
                break
        if not exists:
            components.append(full_state)
            
    if postal_code:
        exists = False
        for c in components:
            if postal_code.lower() in c.lower():
                exists = True
                break
        if not exists:
            components.append(postal_code)
            
    if full_country:
        exists = False
        for c in components:
            if full_country.lower() in c.lower():
                exists = True
                break
        if not exists:
            components.append(full_country)
            
    return ", ".join([c for c in components if c])

def standardize_row(row):
    country = normalize_country(row.get('Country', ''))
    state = normalize_state(row.get('State', ''), country)
    city = clean_value(row.get('City', ''))
    
    address = clean_value(row.get('Address', ''))
    postal_code = ""
    if country == "USA":
        zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', address)
        if zip_match: postal_code = zip_match.group(0)
    elif country == "UK":
        postcode_match = re.search(r'\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b', address, re.I)
        if postcode_match: postal_code = postcode_match.group(0).upper()

    company_name = clean_value(row.get('Company Name', ''))
    
    new_row = {
        'company_name': company_name,
        'country': country,
        'state': state,
        'city': city,
        'email': normalize_email(row.get('Email', '')),
        'phone': normalize_phone(row.get('Phone', ''), country),
        'website': normalize_website(row.get('Website', '')),
        'linkedin_url': normalize_linkedin(row.get('LinkedIn Profile', '')),
        'industry': normalize_industry(row.get('Industry', '')),
        'description': clean_value(row.get('Description', '')),
        'address': format_address(address, city, state, postal_code, country)
    }
    
    reg_num = clean_value(row.get('Registration Number', ''))
    if not reg_num:
        raw = f"{company_name.lower().strip()}|{country.upper()}"
        reg_num = "CSV-" + hashlib.md5(raw.encode()).hexdigest()[:12].upper()
    new_row['registration_number'] = reg_num
    
    return new_row

def main():
    csv_path = os.path.join(ROOT, "backend", "clean_business_records.csv")
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    db = SessionLocal()
    try:
        print(f"Processing {csv_path}...")
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        total = len(rows)
        updated = 0
        inserted = 0
        
        all_biz = db.query(Business).all()
        by_reg = {(b.registration_number, b.country): b for b in all_biz}
        by_name = {(b.company_name.lower().strip(), b.country.upper(), (b.state or "").lower()): b for b in all_biz}
        
        for i, row in enumerate(rows):
            std_row = standardize_row(row)
            
            biz = by_reg.get((std_row['registration_number'], std_row['country']))
            if not biz:
                name_key = (std_row['company_name'].lower().strip(), std_row['country'].upper(), (std_row['state'] or "").lower())
                biz = by_name.get(name_key)
                
            if biz:
                biz.company_name = std_row['company_name']
                biz.registration_number = std_row['registration_number']
                biz.country = std_row['country']
                biz.state = std_row['state']
                biz.city = std_row['city']
                biz.address = std_row['address']
                biz.email = std_row['email'] or biz.email
                biz.phone = std_row['phone'] or biz.phone
                biz.website = std_row['website'] or biz.website
                biz.linkedin_url = std_row['linkedin_url'] or biz.linkedin_url
                biz.industry = std_row['industry'] or biz.industry
                biz.description = std_row['description'] or biz.description
                updated += 1
            else:
                biz = Business(
                    company_name=std_row['company_name'],
                    registration_number=std_row['registration_number'],
                    country=std_row['country'],
                    state=std_row['state'],
                    city=std_row['city'],
                    address=std_row['address'],
                    email=std_row['email'],
                    phone=std_row['phone'],
                    website=std_row['website'],
                    linkedin_url=std_row['linkedin_url'],
                    industry=std_row['industry'],
                    description=std_row['description'],
                    status="Active"
                )
                db.add(biz)
                inserted += 1
                by_reg[(std_row['registration_number'], std_row['country'])] = biz
            
            if (i + 1) % 100 == 0:
                db.commit()

        db.commit()
        print(f"Finished CSV processing. Updated: {updated}, Inserted: {inserted}")
        
        print("Performing final consistency pass on ALL records...")
        all_biz = db.query(Business).all()
        for biz in all_biz:
            changed = False
            
            # Standard normalizations
            new_country = normalize_country(biz.country)
            if new_country != biz.country:
                biz.country = new_country
                changed = True
                
            new_state = normalize_state(biz.state, biz.country)
            if new_state != biz.state:
                biz.state = new_state
                changed = True
                
            new_industry = normalize_industry(biz.industry)
            if new_industry != biz.industry:
                biz.industry = new_industry
                changed = True
                
            new_phone = normalize_phone(biz.phone, biz.country)
            if new_phone != biz.phone:
                biz.phone = new_phone
                changed = True
                
            # Re-format address for ALL records to ensure strict "Street, Area, City, State, Country"
            # But only if it needs cleaning
            if biz.address:
                # Check for messy patterns
                needs_clean = is_garbage_address(biz.address) or re.search(r',\s*[A-Z]{2},\s*[A-Z]{2}', biz.address)
                if not needs_clean:
                    # Also check if it's missing state/country full names
                    if biz.state and biz.state not in biz.address: needs_clean = True
                    if biz.country and biz.country not in biz.address: needs_clean = True
                
                if needs_clean:
                    street = re.sub(r',\s*[A-Z]{2}(?:,\s*[A-Z]{2})*,\s*(?:US|USA|UK|GB)$', '', biz.address, flags=re.I)
                    street = re.sub(r',\s*(?:USA|UK|GB|US)(?:,\s*(?:USA|UK|GB|US))*$', '', street, flags=re.I)
                    if is_garbage_address(street): street = ""
                    biz.address = format_address(street, biz.city, biz.state, "", biz.country)
                    changed = True
            
            if changed:
                db.add(biz)
        
        db.commit()
        print("Final pass completed.")

    finally:
        db.close()

if __name__ == "__main__":
    main()
