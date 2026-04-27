import os
import sys
import logging
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.join(os.getcwd(), "backend"))
from database import SessionLocal
from models.business import Business
from services.enrichment_service import enrich_business


from concurrent.futures import ThreadPoolExecutor

def process_one_contact(biz_id):
    db = SessionLocal()
    try:
        if enrich_business(db, biz_id):
            return True
    except Exception as e:
        logger.error(f"Error enriching {biz_id}: {e}")
    finally:
        db.close()
    return False

def fix_contacts():
    db = SessionLocal()
    try:
        targets = db.query(Business).filter(or_(
            Business.email == "Not Available",
            Business.email == None,
            Business.email == "",
            Business.phone == "Not Available",
            Business.phone == None,
            Business.phone == ""
        )).all()

        biz_ids = [b.id for b in targets]
        print(f"Found {len(biz_ids)} records to enrich. Starting parallel processing (20 threads)...")
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(process_one_contact, biz_ids))




        
        updated_count = sum(1 for r in results if r)
        print(f"Finished! Successfully enriched {updated_count} records with real data.")

    finally:
        db.close()


if __name__ == "__main__":
    fix_contacts()
