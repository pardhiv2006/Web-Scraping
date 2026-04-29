
import os
import sys

# Add backend to sys.path
root_path = os.getcwd()
backend_path = os.path.join(root_path, "backend")
sys.path.insert(0, backend_path)

from database import SessionLocal
from services.scrape_service import run_scrape

def trigger_scrape():
    db = SessionLocal()
    try:
        print("Triggering scrape for UK/England...")
        result = run_scrape(
            country="UK",
            states=["England"],
            db=db,
            user_id=1 # Assuming admin user id 1
        )
        print(f"Scrape result: {result['inserted']} inserted, {result['skipped_dupes']} skipped, {result['errors']} errors")
        print(f"Total records returned: {len(result['records'])}")
        
    finally:
        db.close()

if __name__ == "__main__":
    trigger_scrape()
