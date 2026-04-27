import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add backend to path to import models
sys.path.append(os.path.join(os.getcwd(), "backend"))

from database import DATABASE_URL
from models.business import Business

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def check_missing_data():
    db = SessionLocal()
    try:
        total = db.query(Business).count()
        print(f"Total businesses: {total}")
        
        # Total Incomplete
        missing_criteria = (
            (Business.email == None) | (Business.email == "") | (Business.email == "-") |
            (Business.phone == None) | (Business.phone == "") | (Business.phone == "-") |
            (Business.website == None) | (Business.website == "") | (Business.website == "-") |
            (Business.ceo_name == None) | (Business.ceo_name == "") | (Business.ceo_name == "-") |
            (Business.linkedin_url == None) | (Business.linkedin_url == "") | (Business.linkedin_url == "-") |
            (Business.address == None) | (Business.address == "") | (Business.address == "-") |
            (Business.registration_date == None) | (Business.registration_date == "") | (Business.registration_date == "-")
        )
        total_incomplete = db.query(Business).filter(missing_criteria).count()

        # Check for individual missing fields
        missing_email = db.query(Business).filter((Business.email == None) | (Business.email == "") | (Business.email == "-")).count()
        missing_phone = db.query(Business).filter((Business.phone == None) | (Business.phone == "") | (Business.phone == "-")).count()
        missing_website = db.query(Business).filter((Business.website == None) | (Business.website == "") | (Business.website == "-")).count()
        missing_ceo = db.query(Business).filter((Business.ceo_name == None) | (Business.ceo_name == "") | (Business.ceo_name == "-")).count()
        missing_linkedin = db.query(Business).filter((Business.linkedin_url == None) | (Business.linkedin_url == "") | (Business.linkedin_url == "-")).count()
        missing_address = db.query(Business).filter((Business.address == None) | (Business.address == "") | (Business.address == "-")).count()

        print(f"Total Incomplete Records: {total_incomplete}")
        print(f"Percentage Complete: {((total - total_incomplete) / total * 100):.2f}%")
        print(f"Missing Email: {missing_email}")
        print(f"Missing Phone: {missing_phone}")
        print(f"Missing Website: {missing_website}")
        print(f"Missing CEO Name: {missing_ceo}")
        print(f"Missing LinkedIn: {missing_linkedin}")
        print(f"Missing Address: {missing_address}")


        
    finally:
        db.close()

if __name__ == "__main__":
    check_missing_data()
