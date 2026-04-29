
import os
import sys

# Add backend and root to sys.path
root_path = os.getcwd()
backend_path = os.path.join(root_path, "backend")
sys.path.insert(0, backend_path)
sys.path.insert(0, root_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func
from ingest_csvs import US_STATE_MAP

def fix_mislabeled_records():
    db = SessionLocal()
    try:
        updated_total = 0
        
        # 1. Fix UK England records mislabeled as Scotland/Wales/NI
        uk_records = db.query(Business).filter(
            func.upper(Business.country) == 'UK',
            func.upper(Business.state) != 'ENGLAND',
            Business.address.like('%England%')
        ).all()
        
        print(f"Found {len(uk_records)} mislabeled UK records.")
        for biz in uk_records:
            print(f"Updating {biz.company_name}: {biz.state} -> England")
            biz.state = "England"
            updated_total += 1
            
        # 2. Fix USA mismatches
        usa_records = db.query(Business).filter(func.upper(Business.country) == 'USA').all()
        usa_count = 0
        for biz in usa_records:
            for abbr, name in US_STATE_MAP.items():
                if name.lower() in biz.address.lower() and name.lower() != biz.state.lower():
                    # Check if the state is NOT just a mention but seems to be the primary state
                    # For simplicity, if the address mentions a state that is NOT the current state, we update it.
                    # This is aggressive but helps with consistency.
                    if name in ["New York", "California", "Texas", "Florida", "Georgia", "Washington"]: # High confidence states
                         biz.state = name
                         usa_count += 1
                         break
        print(f"Updated {usa_count} USA records.")
        updated_total += usa_count

        # 3. Fix UAE mismatches
        uae_records = db.query(Business).filter(func.upper(Business.country) == 'UAE').all()
        uae_count = 0
        uae_states = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah", "Umm Al Quwain", "Fujairah"]
        for biz in uae_records:
            for s in uae_states:
                if s.lower() in biz.address.lower() and s.lower() != biz.state.lower():
                    biz.state = s
                    uae_count += 1
                    break
        print(f"Updated {uae_count} UAE records.")
        updated_total += uae_count

        db.commit()
        print(f"\nTotal records updated: {updated_total}")
        
    finally:
        db.close()

if __name__ == "__main__":
    fix_mislabeled_records()
