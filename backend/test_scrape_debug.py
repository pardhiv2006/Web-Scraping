
import os
import sys
import logging
from sqlalchemy.orm import Session

# Add backend to path
backend_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, backend_path)

from database import SessionLocal, init_db
from services.scrape_service import run_scrape

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_scrape_flow(country, states):
    print(f"\n===== Testing Scrape Flow for {country} - {states} =====")
    db = SessionLocal()
    try:
        # We use a dummy user_id if needed, or None
        result = run_scrape(country=country, states=states, db=db, user_id=None)
        
        print("\n--- Scrape Result Summary ---")
        print(f"Total Fetched: {result.get('total_fetched')}")
        print(f"Inserted: {result.get('inserted')}")
        print(f"Skipped Dupes: {result.get('skipped_dupes')}")
        print(f"Errors: {result.get('errors')}")
        print(f"Records Count: {len(result.get('records'))}")
        
        if result.get('records'):
            print("\nFirst record sample:")
            print(result.get('records')[0])
        else:
            print("\n!!! NO RECORDS RETURNED !!!")
            
    except Exception as e:
        print(f"Error during scrape: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    init_db()
    # Test US - CA
    test_scrape_flow("US", ["CA"])
    # Test UK - ENG
    # test_scrape_flow("UK", ["ENG"])
