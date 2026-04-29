
import os
import sys

# Add backend to sys.path
backend_path = os.path.join(os.getcwd(), "backend")
sys.path.insert(0, backend_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func

def check_england_any_country():
    db = SessionLocal()
    try:
        results = db.query(Business.country, func.count(Business.id)).filter(
            func.upper(Business.state) == 'ENGLAND'
        ).group_by(Business.country).all()
        
        print("Records with state 'England':")
        for country, count in results:
            print(f"Country: '{country}', Count: {count}")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_england_any_country()
