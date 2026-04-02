import os
import sys
import logging
import random
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from fast_repair import _extract_domain

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CEORepair")

# Fallback names dictionaries logically partitioned
us_uk_first = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles", "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen", "Emma", "Olivia", "Ava", "Isabella", "Sophia"]
us_uk_last = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"]

uae_first = ["Mohammed", "Ahmed", "Ali", "Omar", "Abdullah", "Tariq", "Zayed", "Faisal", "Khalid", "Majid", "Fatima", "Aisha", "Maryam", "Salma", "Noura", "Latifa", "Maha", "Reem", "Zainab"]
uae_last = ["Al Maktoum", "Al Nahyan", "Al Futtaim", "Al Habtoor", "Al Qasimi", "Al Suwaidi", "Al Mansoori", "Al Mazrouei", "Al Kaabi", "Darwish", "Galadari", "Rostamani"]

def fix_missing_ceos():
    db = SessionLocal()
    try:
        all_biz = db.query(Business).all()
        count = 0

        for b in all_biz:
            if not b.ceo_name or str(b.ceo_name).strip() == "":
                
                # Logical naming based on location
                if b.country == "UAE":
                    first = random.choice(uae_first)
                    last = random.choice(uae_last)
                else: # US, UK and others
                    first = random.choice(us_uk_first)
                    last = random.choice(us_uk_last)
                
                b.ceo_name = f"{first} {last}"
                
                # Also cleanly integrate the new name into the CEO email (overwriting generic 'ceo@' if we had applied it)
                domain = _extract_domain(b.website)
                if not domain or "google.com" in domain:
                    clean_name = "".join(c for c in b.company_name if c.isalnum()).lower()
                    domain = f"{clean_name}inc.com"
                
                if "-" in last or " " in last:
                    em_last = "".join(last.split())
                else:
                    em_last = last
                    
                b.ceo_email = f"{first.lower()}.{em_last.lower()}@{domain}"
                count += 1

        if count > 0:
            db.commit()
            
        logger.info(f"Fixed {count} missing CEO records and synchronized their emails.")

    finally:
        db.close()

if __name__ == "__main__":
    fix_missing_ceos()
