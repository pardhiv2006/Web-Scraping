"""
One-off script to fill missing registration dates with synthetic data.
"""
import sys
import os
import random
from datetime import datetime, timedelta

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import SessionLocal
from models.business import Business

# Values treated as "blank / empty" for quality filtering
_EMPTY = {'', '-', '--', 'n/a', 'N/A', 'none', 'None', 'null', 'NULL',
          'tbd', 'TBD', 'not available', 'unknown', 'na', 'NA', 'N/a'}

def _is_blank(value) -> bool:
    return not value or str(value).strip() in _EMPTY

def _get_synthetic_reg_date() -> str:
    """Generate a random ISO date within the last 30-270 days."""
    days_ago = random.randint(30, 270)
    target_date = datetime.now() - timedelta(days=days_ago)
    return target_date.strftime("%Y-%m-%d")

def main():
    db = SessionLocal()
    try:
        # Find records with missing registration dates
        records = db.query(Business).all()
        to_update = [r for r in records if _is_blank(r.registration_date)]
        
        print(f"Found {len(to_update)} records with missing registration dates.")
        
        count = 0
        for r in to_update:
            r.registration_date = _get_synthetic_reg_date()
            count += 1
            
        db.commit()
        print(f"Successfully updated {count} records with synthetic dates.")
        
    except Exception as e:
        print(f"Error during update: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    main()
