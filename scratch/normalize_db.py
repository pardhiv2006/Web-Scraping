import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.join(os.getcwd(), "backend"))
from database import DATABASE_URL
from models.business import Business
from models.search_history import SearchHistory
from models.user import User 

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def normalize_db():
    db = SessionLocal()
    try:
        # 1. Normalize Countries
        country_map = {
            "UNITED ARAB EMIRATES": "UAE",
            "UNITED KINGDOM": "UK",
            "GREAT BRITAIN": "UK",
            "UNITED STATES": "USA",
            "UNITED STATES OF AMERICA": "USA",
            "US": "USA"
        }
        
        # 2. Normalize UAE States
        uae_state_map = {
            "DXB": "Dubai",
            "AUH": "Abu Dhabi",
            "SHJ": "Sharjah",
            "AJM": "Ajman",
            "RAK": "Ras Al Khaimah",
            "UAQ": "Umm Al Quwain",
            "FUJ": "Fujairah"
        }

        print("Normalizing businesses...")
        bizs = db.query(Business).all()
        count = 0
        for b in bizs:
            changed = False
            # Normalize country
            if b.country:
                new_c = country_map.get(b.country.upper())
                if new_c: 
                    b.country = new_c
                    changed = True
            
            # Normalize state
            if b.country == "UAE" and b.state:
                new_s = uae_state_map.get(b.state.upper())
                if new_s: 
                    b.state = new_s
                    changed = True
            
            # Normalize other states to Title Case if they are all caps
            if b.state and b.state.isupper() and len(b.state) > 3:
                b.state = b.state.title()
                changed = True
            
            if changed: count += 1

        print(f"Updated {count} businesses.")

        print("Normalizing history...")
        histories = db.query(SearchHistory).all()
        h_count = 0
        for h in histories:
            if h.country:
                new_c = country_map.get(h.country.upper())
                if new_c: 
                    h.country = new_c
                    h_count += 1

        print(f"Updated {h_count} history items.")

        db.commit()
        print("Database normalization complete.")
        
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    normalize_db()
