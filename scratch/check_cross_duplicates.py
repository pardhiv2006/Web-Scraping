
import os
import sys

# Add backend to sys.path
backend_path = os.path.join(os.getcwd(), "backend")
sys.path.insert(0, backend_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func

def check_for_cross_duplicates():
    db = SessionLocal()
    try:
        # Get all names in England
        england_names = {r[0].lower().strip() for r in db.query(Business.company_name).filter(
            func.upper(Business.country) == 'UK',
            func.upper(Business.state) == 'ENGLAND'
        ).all()}
        
        # Get mislabeled UK records
        mislabeled = db.query(Business.id, Business.company_name, Business.state, Business.address).filter(
            func.upper(Business.country) == 'UK',
            func.upper(Business.state) != 'ENGLAND',
            Business.address.like('%England%')
        ).all()
        
        print(f"Checking {len(mislabeled)} mislabeled records for duplicates in England...")
        for rid, name, state, addr in mislabeled:
            if name.lower().strip() in england_names:
                print(f"DUPLICATE: '{name}' already exists in England! (ID {rid} in {state})")
            else:
                print(f"NEW: '{name}' is not in England yet.")
                
    finally:
        db.close()

if __name__ == "__main__":
    check_for_cross_duplicates()
