import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.join(os.getcwd(), "backend"))
from database import DATABASE_URL
from models.business import Business
from models.search_history import SearchHistory

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def audit_uae():
    db = SessionLocal()
    try:
        uae_records = db.query(Business).filter(Business.country == "UAE").count()
        uae_history = db.query(SearchHistory).filter(SearchHistory.country == "UAE").count()
        
        print(f"UAE Records in Business table: {uae_records}")
        print(f"UAE History items: {uae_history}")
        
        # Check all columns for ANY blank in the whole DB
        all_cols = ["company_name", "registration_number", "registration_date", "status", "address", "country", "state", "email", "phone", "website", "ceo_name", "founder_name", "ceo_email", "linkedin_url", "source_url"]
        
        for col in all_cols:
            count = db.query(Business).filter((getattr(Business, col) == None) | (getattr(Business, col) == "") | (getattr(Business, col) == "-")).count()
            if count > 0:
                print(f"Column '{col}' has {count} blanks/placeholders.")
        
    finally:
        db.close()

if __name__ == "__main__":
    audit_uae()
