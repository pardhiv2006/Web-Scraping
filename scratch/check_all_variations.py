
import os
import sys

# Add backend to sys.path
backend_path = os.path.join(os.getcwd(), "backend")
sys.path.insert(0, backend_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func

def check_all_variations():
    db = SessionLocal()
    try:
        print("Checking for NULL or empty country/state:")
        null_country = db.query(Business).filter(Business.country == None).count()
        empty_country = db.query(Business).filter(Business.country == "").count()
        null_state = db.query(Business).filter(Business.state == None).count()
        empty_state = db.query(Business).filter(Business.state == "").count()
        
        print(f"NULL Country: {null_country}")
        print(f"Empty Country: {empty_country}")
        print(f"NULL State: {null_state}")
        print(f"Empty State: {empty_state}")
        
        print("\nAll unique country values:")
        countries = db.query(Business.country).distinct().all()
        for c in countries:
            print(f"'{c[0]}'")
            
        print("\nAll unique state values (first 20):")
        states = db.query(Business.state).distinct().limit(20).all()
        for s in states:
            print(f"'{s[0]}'")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_all_variations()
