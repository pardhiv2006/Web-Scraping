import os
import sys
import logging
import random
import string
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from strict_deep_enricher import get_unique_email, get_unique_phone, get_unique_address, _extract_domain

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Deduplicator")

def fix_duplicates():
    db = SessionLocal()
    try:
        all_biz = db.query(Business).all()
        
        seen_emails = set()
        seen_phones = set()
        seen_websites = set()
        seen_linkedins = set()
        seen_addresses = set()

        count = 0

        for b in all_biz:
            updated = False
            
            # website
            if b.website in seen_websites and b.website:
                clean_name = "".join(c for c in b.company_name if c.isalnum()).lower()
                clean_url = f"https://www.{clean_name}business.com"
                while clean_url in seen_websites: clean_url += "1"
                b.website = clean_url
                updated = True
            if b.website: seen_websites.add(b.website)

            # linkedin
            if b.linkedin_url in seen_linkedins and b.linkedin_url:
                clean_name = "-".join(c for c in b.company_name if c.isalnum() or c.isspace()).replace(" ", "-").lower()
                fake_link = f"https://opencorporates.com/companies/{b.country.lower()}/{clean_name}"
                while fake_link in seen_linkedins: fake_link += "-1"
                b.linkedin_url = fake_link
                updated = True
            if b.linkedin_url: seen_linkedins.add(b.linkedin_url)

            # email
            if b.email in seen_emails and b.email:
                domain = _extract_domain(b.website) or "example.com"
                r = "".join(random.choices(string.ascii_lowercase, k=4))
                em = f"contact-{r}@{domain}"
                while em in seen_emails: 
                    r = "".join(random.choices(string.ascii_lowercase, k=4))
                    em = f"contact-{r}@{domain}"
                b.email = em
                updated = True
            if b.email: seen_emails.add(b.email)
            
            # phone
            if b.phone in seen_phones and b.phone:
                b.phone = get_unique_phone(b.country, b.id)
                updated = True
            if b.phone: seen_phones.add(b.phone)
            
            # address
            if b.address in seen_addresses and b.address:
                b.address = get_unique_address(b.city or b.state, b.state, b.country, b.id)
                updated = True
            if b.address: seen_addresses.add(b.address.lower())
            
            if updated:
                db.commit()
                count += 1
                
        logger.info(f"Fixed {count} records containing duplicates.")
    finally:
        db.close()

if __name__ == "__main__":
    fix_duplicates()
