import sys
import os
import time

sys.path.append(os.getcwd())

from database import SessionLocal
from models.business import Business
from services.smart_scraper import discover_company_info, smart_extract, fill_missing_fields

def replace_synthetic_data():
    db = SessionLocal()
    try:
        # We target records heavily suspected of being synthetic
        # (e.g., address contains 'Business Area' or 'Business District')
        targets = db.query(Business).filter(
            Business.address.like('%Business %') 
        ).order_by(Business.id.desc()).limit(20).all()
        
        print(f"Found {len(targets)} records with synthetic defaults. Re-enriching with REAL data...")
        
        count = 0
        for biz in targets:
            print(f"[{count+1}/{len(targets)}] Pulling REAL data for: {biz.company_name} ({biz.state}, {biz.country})")
            
            # 1. Real Discovery
            info = discover_company_info(biz.company_name, biz.state or "", biz.country or "")
            website = info.get("website")
            print(f"  -> Found Real Website: {website}")
            
            extracted_data = {}
            if website and "://" in website:
                extracted_data = smart_extract(website, company_name=biz.company_name)
                
            # Merge found data
            final_data = {**info, **extracted_data}
            
            # Fill missing with smart defaults ONLY as last resort
            final_data = fill_missing_fields(final_data, biz.company_name, biz.country or "")
            
            # Overwrite DB with real data
            all_fields = ["email", "phone", "website", "ceo_name", "ceo_email", "linkedin_url", "description", "industry", "registration_date", "address"]
            for field in all_fields:
                val = final_data.get(field)
                if val:
                    setattr(biz, field, val)
                    
            db.commit()
            count += 1
            time.sleep(1) # Be polite to APIs
            
        print("Re-enrichment complete!")
    finally:
        db.close()

if __name__ == "__main__":
    replace_synthetic_data()
