
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

def normalize_and_sync():
    db = SessionLocal()
    try:
        print("Starting Data Synchronization...")
        
        # 1. State Normalization (Address -> State)
        # UK
        uk_fixes = 0
        uk_mislabeled = db.query(Business).filter(
            func.upper(Business.country) == 'UK',
            Business.address.like('%England%')
        ).all()
        for biz in uk_mislabeled:
            if biz.state != 'England':
                biz.state = 'England'
                uk_fixes += 1
        
        # USA
        usa_fixes = 0
        usa_records = db.query(Business).filter(func.upper(Business.country) == 'USA').all()
        for biz in usa_records:
            for abbr, name in US_STATE_MAP.items():
                if f", {name}" in biz.address or f" {name}," in biz.address or biz.address.endswith(name):
                    if biz.state != name:
                        biz.state = name
                        usa_fixes += 1
                        break
        
        # UAE
        uae_fixes = 0
        uae_states = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah", "Umm Al Quwain", "Fujairah"]
        uae_records = db.query(Business).filter(func.upper(Business.country) == 'UAE').all()
        for biz in uae_records:
            for s in uae_states:
                if s in biz.address and biz.state != s:
                    biz.state = s
                    uae_fixes += 1
                    break
        
        print(f"Fixed States: UK={uk_fixes}, USA={usa_fixes}, UAE={uae_fixes}")
        db.commit()
        
        # 2. Deduplication (Company Name + State + Country)
        print("Deduplicating...")
        all_biz = db.query(Business).order_by(Business.id.desc()).all()
        seen = set()
        deleted = 0
        for biz in all_biz:
            key = (biz.company_name.lower().strip(), (biz.state or "").lower().strip(), biz.country.upper())
            if key in seen:
                db.delete(biz)
                deleted += 1
            else:
                seen.add(key)
        
        db.commit()
        print(f"Deleted {deleted} duplicates.")
        
        # 3. Final Count Check
        england_count = db.query(Business).filter(
            func.upper(Business.country) == 'UK',
            func.upper(Business.state) == 'ENGLAND'
        ).count()
        print(f"\nFinal UK England record count: {england_count}")
        
        total_uk = db.query(Business).filter(func.upper(Business.country) == 'UK').count()
        total_usa = db.query(Business).filter(func.upper(Business.country) == 'USA').count()
        total_uae = db.query(Business).filter(func.upper(Business.country) == 'UAE').count()
        
        print(f"Final Totals: UK={total_uk}, USA={total_usa}, UAE={total_uae}")
        
    finally:
        db.close()

if __name__ == "__main__":
    normalize_and_sync()
