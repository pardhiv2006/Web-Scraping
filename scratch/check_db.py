
import os
import sys

# Add backend to sys.path
backend_path = os.path.join(os.getcwd(), "backend")
sys.path.insert(0, backend_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func

def check_counts():
    db = SessionLocal()
    try:
        # Total records
        total = db.query(Business).count()
        print(f"Total records in DB: {total}")
        
        # UK England records
        uk_england = db.query(Business).filter(
            func.upper(Business.country) == 'UK',
            func.upper(Business.state) == 'ENGLAND'
        ).count()
        print(f"UK England records: {uk_england}")
        
        # Check all countries/states
        results = db.query(Business.country, Business.state, func.count(Business.id)).group_by(Business.country, Business.state).all()
        print("\nBreakdown by Country/State:")
        for country, state, count in results:
            print(f"{country} - {state}: {count}")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_counts()
