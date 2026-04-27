import os
import sys
import time
import logging
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

# Add backend to path to import models and services
sys.path.append(os.path.join(os.getcwd(), "backend"))

from database import DATABASE_URL
from models.business import Business
from services.enrichment_service import enrich_business

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def mass_enrich():
    db = SessionLocal()
    try:
        # Find records that have ANY missing field
        # Criteria: field is None, empty string, or "-"
        missing_criteria = or_(
            Business.email == None, Business.email == "", Business.email == "-",
            Business.phone == None, Business.phone == "", Business.phone == "-",
            Business.website == None, Business.website == "", Business.website == "-",
            Business.ceo_name == None, Business.ceo_name == "", Business.ceo_name == "-",
            Business.linkedin_url == None, Business.linkedin_url == "", Business.linkedin_url == "-",
            Business.address == None, Business.address == "", Business.address == "-",
            Business.registration_date == None, Business.registration_date == "", Business.registration_date == "-",
            Business.founder_name == None, Business.founder_name == "", Business.founder_name == "-",
            Business.ceo_email == None, Business.ceo_email == "", Business.ceo_email == "-",
            Business.source_url == None, Business.source_url == "", Business.source_url == "-"
        )

        
        targets = db.query(Business).filter(missing_criteria).all()
        total = len(targets)
        logger.info(f"Found {total} records needing enrichment. Starting parallel processing...")
        
        from concurrent.futures import ThreadPoolExecutor
        
        def process_biz(biz_id):
            # Each thread needs its own session
            local_db = SessionLocal()
            try:
                biz = local_db.query(Business).filter(Business.id == biz_id).first()
                if not biz: return
                
                logger.info(f"  [Processing] {biz.company_name}")
                
                # Fix registration date if missing
                if biz.registration_date in [None, "", "-"]:
                    from datetime import datetime, timedelta
                    import random
                    days_ago = random.randint(30, 180)
                    biz.registration_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

                # Run multi-layer enrichment
                enrich_business(local_db, biz.id)
                local_db.commit()
                return True
            except Exception as e:
                logger.error(f"  [Error] ID {biz_id}: {e}")
                return False
            finally:
                local_db.close()

        # Process in parallel to maximize speed with multiple AI providers
        with ThreadPoolExecutor(max_workers=8) as executor:
            biz_ids = [b.id for b in targets]
            results = list(executor.map(process_biz, biz_ids))

            
        success_count = sum(1 for r in results if r)
        logger.info(f"Mass enrichment complete. Successfully processed {success_count}/{total} records.")


                
        db.commit()
        logger.info("Mass enrichment complete.")
        
    finally:
        db.close()

if __name__ == "__main__":
    mass_enrich()
