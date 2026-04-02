
import sys
import os
import threading
import time
from sqlalchemy import or_

# Add current directory to path
sys.path.append(os.getcwd())

from database import SessionLocal
from models.business import Business
from services.scrape_service import enrich_business_background

def mass_enrich_all_blanks():
    db = SessionLocal()
    try:
        # Query for any record that has a blank in any of the core fields
        targets = db.query(Business).filter(
            or_(
                Business.website == None, Business.website == "",
                Business.email == None, Business.email == "",
                Business.phone == None, Business.phone == "",
                Business.ceo_name == None, Business.ceo_name == "",
                Business.ceo_email == None, Business.ceo_email == "",
                Business.linkedin_url == None, Business.linkedin_url == "",
                Business.description == None, Business.description == "",
                Business.industry == None, Business.industry == "",
                Business.registration_date == None, Business.registration_date == "",
                Business.address == None, Business.address == ""
            )
        ).all()

        print(f"Found {len(targets)} records with blank fields. Starting mass enrichment...")
        
        # We'll run them in small batches to manage threads and DB connections
        batch_size = 5
        for i in range(0, len(targets), batch_size):
            batch = targets[i:i + batch_size]
            threads = []
            print(f"Processing Batch {i//batch_size + 1}... ({i} to {i+len(batch)})")
            
            for biz in batch:
                t = threading.Thread(target=enrich_business_background, args=(biz.id,))
                t.start()
                threads.append(t)
            
            # Wait for batch to finish before next one
            for t in threads:
                t.join()
                
            print(f"Batch {i//batch_size + 1} completed. Sleeping to avoid API rate limits...")
            time.sleep(3.0) # Increased rest to prevent DDGS rate limiting

    finally:
        db.close()
    
    print("--- MASS ENRICHMENT COMPLETE ---")

if __name__ == "__main__":
    mass_enrich_all_blanks()
