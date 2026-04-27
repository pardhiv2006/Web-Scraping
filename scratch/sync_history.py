import os
import sys
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.join(os.getcwd(), "backend"))
from database import DATABASE_URL
from models.business import Business
from models.search_history import SearchHistory
from models.user import User

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def sync_history():
    db = SessionLocal()
    try:
        print("Starting History Sync...")
        histories = db.query(SearchHistory).all()
        
        for h in histories:
            if not h.result_data: continue
            
            data = json.loads(h.result_data)
            updated = False
            
            new_data = []
            for record in data:
                # Find the latest data in Business table
                biz = db.query(Business).filter(Business.company_name == record.get("company_name")).first()
                if biz:
                    new_data.append(biz.to_dict())
                    updated = True
                else:
                    new_data.append(record)
            
            if updated:
                h.result_data = json.dumps(new_data)
                print(f"  [Synced] History ID {h.id} ({h.country})")
        
        db.commit()
        print("History synchronization complete.")
        
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    sync_history()
