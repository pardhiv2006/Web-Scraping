
import csv
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

def find_missing_england_records():
    db = SessionLocal()
    try:
        # Get all registration numbers for UK England in DB
        db_records = db.query(Business.company_name).filter(
            func.upper(Business.country) == 'UK',
            func.upper(Business.state) == 'ENGLAND'
        ).all()
        db_names = {r[0].lower().strip() for r in db_records}
        
        print(f"DB names for England: {len(db_names)}")
        
        csv_path = "backend/clean_business_records.csv"
        missing_but_valid = []
        garbage_filtered = []
        
        from ingest_csvs import is_garbage_company, normalise_country, normalise_state
        
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("Company Name") or "").strip()
                if not name: continue
                
                country = normalise_country(row.get("Country") or "")
                state = normalise_state(row.get("State") or "", country or "")
                
                if country == "UK" and state == "England":
                    name_lower = name.lower()
                    if name_lower not in db_names:
                        if is_garbage_company(name):
                            garbage_filtered.append(name)
                        else:
                            missing_but_valid.append(name)
                            
        print(f"\nMissing but 'valid' (according to current logic): {len(missing_but_valid)}")
        for m in missing_but_valid[:10]:
            print(f" - {m}")
            
        print(f"\nFiltered as 'garbage': {len(garbage_filtered)}")
        for g in garbage_filtered[:10]:
            print(f" - {g}")
            
    finally:
        db.close()

if __name__ == "__main__":
    find_missing_england_records()
