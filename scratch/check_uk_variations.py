
import os
import sys

# Add backend to sys.path
backend_path = os.path.join(os.getcwd(), "backend")
sys.path.insert(0, backend_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func

def check_uk_records():
    db = SessionLocal()
    try:
        # Check all records where country is UK (case insensitive)
        results = db.query(Business.state, func.count(Business.id)).filter(
            func.upper(Business.country) == 'UK'
        ).group_by(Business.state).all()
        
        print("UK State breakdown:")
        for state, count in results:
            print(f"'{state}': {count}")
            
        # Check for any records that might be UK but have different country string
        others = db.query(Business.country, Business.state, func.count(Business.id)).filter(
            Business.country.like('%UK%') | Business.country.like('%United Kingdom%')
        ).group_by(Business.country, Business.state).all()
        
        print("\nPossible UK variations:")
        for country, state, count in others:
            print(f"Country: '{country}', State: '{state}', Count: {count}")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_uk_records()
