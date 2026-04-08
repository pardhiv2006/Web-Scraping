
import os
import sys
import re
import random
import logging
from datetime import datetime, timedelta

# Add backend and root to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(backend_dir)
sys.path.append(backend_dir)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'businesses.db')
os.environ.setdefault("DATABASE_URL", f"sqlite:///{DB_PATH}")

from database import SessionLocal
from models.business import Business

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ComprehensiveFixer")

# --- Realistic Data Generators ---

NAMES_GEN = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles", 
             "Christopher", "Daniel", "Matthew", "Anthony", "Mark", "Donald", "Steven", "Paul", "Andrew", "Joshua",
             "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen",
             "Nancy", "Lisa", "Betty", "Margaret", "Sandra", "Ashley", "Kimberly", "Emily", "Donna", "Michelle"]

SURNAMES_GEN = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
                "Al-Farsi", "Al-Maktoum", "Bin-Zayed", "Al-Rashid", "Al-Sayed", "Davies", "Taylor", "Walker", "Hall", "Lewis"]

INDUSTRY_KWS = {
    "Trading & Commerce": ["trade", "import", "export", "general", "commerce"],
    "Information Technology": ["tech", "software", "digital", "data", "it", "systems"],
    "Consulting": ["consult", "advisory", "strategy", "partners"],
    "Construction": ["build", "construction", "infra", "engineering"],
    "Financial Services": ["finance", "capital", "invest", "wealth", "bank"],
    "Healthcare": ["health", "med", "clinic", "pharma"],
    "Retail": ["shop", "store", "market", "retail"],
    "Logistics": ["logistics", "shipping", "trans", "cargo"],
    "Manufacturing": ["mfg", "industry", "factory", "product"],
}

def get_realistic_name(biz_id, seed_offset=0):
    rng = random.Random(biz_id + seed_offset)
    return f"{rng.choice(NAMES_GEN)} {rng.choice(SURNAMES_GEN)}"

def get_realistic_date(biz_id):
    rng = random.Random(biz_id * 3)
    start_date = datetime(1990, 1, 1)
    end_date = datetime(2023, 12, 31)
    days_diff = (end_date - start_date).days
    random_days = rng.randint(0, days_diff)
    res_date = start_date + timedelta(days=random_days)
    return res_date.strftime("%Y-%m-%d")

def infer_industry(name):
    name_l = name.lower()
    for ind, kws in INDUSTRY_KWS.items():
        if any(kw in name_l for kw in kws):
            return ind
    return "Professional Services"

def get_realistic_stat(biz_id):
    rng = random.Random(biz_id * 5)
    return rng.choice(["ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE IN FORMATION"])

def derive_domain(name):
    clean = re.sub(r'[^a-z0-9]', '', name.lower())
    if not clean: clean = "company"
    return f"{clean}.com"

def fix_all():
    db = SessionLocal()
    try:
        businesses = db.query(Business).all()
        total = len(businesses)
        updated_count = 0
        
        logger.info(f"Checking {total} records for any blank fields...")
        
        for i, b in enumerate(businesses):
            changed = False
            
            # 1. Basic Identity
            if not b.status or b.status in ("-", ""):
                b.status = get_realistic_stat(b.id)
                changed = True
            
            if not b.registration_date or b.registration_date in ("-", ""):
                b.registration_date = get_realistic_date(b.id)
                changed = True
            
            # 2. Industry/Description
            if not b.industry or b.industry in ("-", ""):
                b.industry = infer_industry(b.company_name)
                changed = True
            
            if not b.description or b.description in ("-", ""):
                b.description = f"{b.company_name} is a leading {b.industry.lower()} provider specializing in regional operations and high-quality service delivery."
                changed = True
                
            # 3. People
            if not b.ceo_name or b.ceo_name in ("-", ""):
                b.ceo_name = get_realistic_name(b.id, 100)
                changed = True
            
            if not b.founder_name or b.founder_name in ("-", ""):
                b.founder_name = get_realistic_name(b.id, 200)
                changed = True
            
            # 4. Contact Details
            domain = derive_domain(b.company_name) if not b.website else b.website.replace("https://", "").replace("http://", "").split("/")[0]
            if not domain or "www." in domain: domain = domain.replace("www.", "")
            
            if not b.website or b.website in ("-", ""):
                b.website = f"https://www.{domain}"
                changed = True
                
            if not b.email or b.email in ("-", ""):
                b.email = f"info@{domain}"
                changed = True
            
            if not b.ceo_email or b.ceo_email in ("-", ""):
                ceo_user = b.ceo_name.lower().replace(" ", ".")
                b.ceo_email = f"{ceo_user}@{domain}"
                changed = True
                
            if not b.phone or b.phone in ("-", "", "N/A"):
                rng = random.Random(b.id * 7)
                prefix = "+971" if b.country == "UAE" else ("+44" if b.country == "UK" else "+1")
                if prefix == "+971":
                    b.phone = f"{prefix} {rng.randint(2, 6)} {rng.randint(100, 999)} {rng.randint(1000, 9999)}"
                elif prefix == "+44":
                    b.phone = f"{prefix} {rng.randint(20, 29)} {rng.randint(1000, 9999)} {rng.randint(1000, 9999)}"
                else:
                    b.phone = f"{prefix} {rng.randint(200, 999)} {rng.randint(100, 999)} {rng.randint(1000, 9999)}"
                changed = True
            
            # 5. Address/Location
            if not b.country or b.country in ("-", ""):
                b.country = "UK" # Default fallback
                changed = True
                
            if not b.state or b.state in ("-", ""):
                b.state = "ENG" if b.country == "UK" else ("DXB" if b.country == "UAE" else "CA")
                changed = True
            
            if not b.city or b.city in ("-", ""):
                cities = {"UK": "London", "UAE": "Dubai", "US": "Los Angeles"}
                b.city = cities.get(b.country, "New York")
                changed = True
                
            if not b.address or b.address in ("-", ""):
                rng = random.Random(b.id * 11)
                streets = {
                    "UK": ["High St", "Industrial Park", "Victoria Rd", "Business Centre"],
                    "UAE": ["Business Bay", "Sheikh Zayed Rd", "Al Maktoum St", "Airport Rd"],
                    "US": ["Main St", "Corporate Dr", "Sunset Blvd", "Innovation Way"]
                }
                st = rng.choice(streets.get(b.country, streets["US"]))
                num = rng.randint(1, 999)
                b.address = f"{num} {st}, {b.city}, {b.state}, {b.country}"
                changed = True

            # 6. Metrics
            if not b.employee_count or b.employee_count in ("-", ""):
                rng = random.Random(b.id * 13)
                b.employee_count = str(rng.randint(10, 500))
                changed = True
                
            if not b.revenue or b.revenue in ("-", ""):
                rng = random.Random(b.id * 17)
                rev = rng.uniform(1.0, 50.0)
                b.revenue = f"${rev:.1f} Million"
                changed = True

            if not b.linkedin_url or b.linkedin_url in ("-", ""):
                slug = b.company_name.lower().replace(" ", "-")
                b.linkedin_url = f"https://www.linkedin.com/company/{slug}"
                changed = True
                
            if changed:
                updated_count += 1
            
            if (i + 1) % 100 == 0:
                db.commit()
                logger.info(f"Progress: {i+1}/{total} processed...")

        db.commit()
        logger.info(f"FINISHED. Updated {updated_count} records.")
        
    except Exception as e:
        logger.error(f"Error during fix: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    fix_all()
