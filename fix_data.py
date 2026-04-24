from backend.database import SessionLocal
from backend.models.business import Business
from sqlalchemy import or_
import os
import sys

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

UK_STATE_MAP = {
    "SCT": "Scotland",
    "SCOTLAND": "Scotland",
    "ENG": "England",
    "ENGLAND": "England",
    "WLS": "Wales",
    "WALES": "Wales",
    "NIR": "Northern Ireland",
    "NORTHERN IRELAND": "Northern Ireland",
    "NEWCASTLE UPON TYNE": "England"
}

US_STATE_MAP = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
}

def fix():
    db = SessionLocal()
    
    # 1. Remove India and South Africa
    deleted_countries = db.query(Business).filter(
        or_(
            Business.country.ilike('india'),
            Business.country.ilike('south africa')
        )
    ).all()
    for b in deleted_countries:
        db.delete(b)
    print(f"Removed {len(deleted_countries)} records from India/South Africa.")
    
    # 2. Normalize States
    businesses = db.query(Business).all()
    normalized_count = 0
    for b in businesses:
        changed = False
        state = b.state.strip() if b.state else None
        
        if b.country == "UK" and state:
            if state.upper() in UK_STATE_MAP:
                b.state = UK_STATE_MAP[state.upper()]
                changed = True
        
        if b.country == "US" and state:
            if state.upper() in US_STATE_MAP:
                b.state = US_STATE_MAP[state.upper()]
                changed = True
            elif state.upper() == "DC":
                b.state = "District of Columbia"
                changed = True
                
        # 3. Infer state from address if missing
        if not b.state and b.address:
            addr = b.address.lower()
            if b.country == "UK":
                if "scotland" in addr or "glasgow" in addr or "edinburgh" in addr or "aberdeen" in addr:
                    b.state = "Scotland"
                    changed = True
                elif "england" in addr or "london" in addr or "manchester" in addr or "birmingham" in addr:
                    b.state = "England"
                    changed = True
                elif "wales" in addr or "cardiff" in addr or "swansea" in addr:
                    b.state = "Wales"
                    changed = True
                elif "northern ireland" in addr or "belfast" in addr:
                    b.state = "Northern Ireland"
                    changed = True
            elif b.country == "US":
                for abbr, name in US_STATE_MAP.items():
                    if f" {abbr} " in b.address.upper() or f", {abbr}" in b.address.upper() or name.lower() in addr:
                        b.state = name
                        changed = True
                        break
        
        if changed:
            normalized_count += 1
            
    db.commit()
    print(f"Normalized/Inferred {normalized_count} state records.")
    
    # 4. Deduplicate properly (by Name + Country)
    all_records = db.query(Business).all()
    seen = set()
    dup_deleted = 0
    for b in all_records:
        key = (b.company_name.lower().strip(), b.country.upper().strip())
        if key in seen:
            db.delete(b)
            dup_deleted += 1
        else:
            seen.add(key)
    
    db.commit()
    print(f"Removed {dup_deleted} duplicates based on Name+Country.")
    
    # Final Scotland check
    scotland_count = db.query(Business).filter(Business.country == 'UK', Business.state == 'Scotland').count()
    print(f"Final Scotland record count: {scotland_count}")
    
    db.close()

if __name__ == "__main__":
    fix()
