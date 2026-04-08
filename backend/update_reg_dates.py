
import os
import sys
import random
from datetime import datetime, timedelta

# Add backend to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ.setdefault("DATABASE_URL", f"sqlite:///{os.path.join(os.path.dirname(backend_dir), 'businesses.db')}")

from database import SessionLocal
from models.business import Business

def update_dates():
    db = SessionLocal()
    try:
        # Range: 2025-06-01 to 2026-04-07
        start_date = datetime(2025, 6, 1)
        end_date = datetime(2026, 4, 7)
        delta_days = (end_date - start_date).days
        
        businesses = db.query(Business).order_by(Business.id).all()
        total = len(businesses)
        print(f"Updating {total} records with dates between {start_date.date()} and {end_date.date()}")
        
        prev_date_str = None
        updated_count = 0
        
        for i, biz in enumerate(businesses):
            # Pick a random date in range
            offset = random.randint(0, delta_days)
            new_date = start_date + timedelta(days=offset)
            new_date_str = new_date.strftime("%Y-%m-%d")
            
            # Ensure no consecutive same date
            attempts = 0
            while new_date_str == prev_date_str and attempts < 10:
                offset = random.randint(0, delta_days)
                new_date = start_date + timedelta(days=offset)
                new_date_str = new_date.strftime("%Y-%m-%d")
                attempts += 1
            
            biz.registration_date = new_date_str
            prev_date_str = new_date_str
            updated_count += 1
            
            if (i + 1) % 200 == 0:
                db.commit()
                print(f"Progress: {i+1}/{total} updated...")
                
        db.commit()
        print(f"FINISHED. Successfully updated {updated_count} records with unique consecutive dates.")
        
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    update_dates()
