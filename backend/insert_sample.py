import os
import sys

# Add backend to path for imports
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, backend_dir)

from database import SessionLocal
from models.business import Business
from datetime import datetime

def insert_sample_data():
    db = SessionLocal()
    try:
        # Check if record already exists
        existing = db.query(Business).filter(Business.company_name == "Integrated Test Corp").first()
        if existing:
            print("Sample record already exists. Updating it...")
            existing.industry = "Software & AI"
            existing.employee_count = "250-500"
            existing.revenue = "$75M USD"
            existing.country = "UK"
            existing.state = "WALES"
            existing.registration_number = "TEST-001"
            db.commit()
            print("Updated existing record.")
        else:
            print("Inserting a new sample record...")
            sample = Business(
                company_name="Integrated Test Corp",
                registration_number="TEST-001",
                country="UK",
                state="WALES",
                registration_date="2026-04-01",
                address="123 Tech Avenue, Cardiff, CF10 1AA, UK",
                status="Active",
                email="contact@integratedtest.com",
                phone="+44 20 7946 0958",
                website="www.integratedtest.com",
                ceo_name="Jane Doe",
                ceo_email="jane@integratedtest.com",
                industry="Software & AI",
                employee_count="250-500",
                revenue="$75M USD",
                linkedin_url="https://www.linkedin.com/company/integrated-test-corp"
            )
            db.add(sample)
            db.commit()
            print("Successfully inserted sample record.")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    insert_sample_data()
