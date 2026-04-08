import os
import sys

# Ensure backend directory is on the path for imports
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, backend_dir)

from database import SessionLocal
from models.business import Business

def test_db():
    db = SessionLocal()
    try:
        print("Querying businesses...")
        records = db.query(Business).limit(5).all()
        print(f"Found {len(records)} records.")
        for r in records:
            print(f"ID: {r.id}, Name: {r.company_name}, Revenue: {getattr(r, 'revenue', 'N/A')}")
            # Test to_dict
            d = r.to_dict()
            print(f"to_dict revenue: {d.get('revenue')}")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    test_db()
