import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.join(os.getcwd(), "backend"))
from database import DATABASE_URL
from models.business import Business

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def check_one():
    db = SessionLocal()
    biz = db.query(Business).filter(Business.company_name == "Arizona llc").first()
    if biz:
        print(f"Company: {biz.company_name}")
        print(f"Email: '{biz.email}'")
        print(f"Phone: '{biz.phone}'")
        print(f"Website: '{biz.website}'")
        print(f"CEO: '{biz.ceo_name}'")
        print(f"Reg Date: '{biz.registration_date}'")
        
        # Check if it satisfies the 'missing' criteria
        is_missing = (
            biz.email in [None, "", "-"] or
            biz.phone in [None, "", "-"] or
            biz.website in [None, "", "-"] or
            biz.ceo_name in [None, "", "-"] or
            biz.registration_date in [None, "", "-"]
        )
        print(f"Is considered missing: {is_missing}")
    else:
        print("Not found")
    db.close()

if __name__ == "__main__":
    check_one()
