
import os
import sys

# Add backend and root to sys.path
root_path = os.getcwd()
backend_path = os.path.join(root_path, "backend")
sys.path.insert(0, backend_path)
sys.path.insert(0, root_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func

def check_mislabeled_all():
    db = SessionLocal()
    try:
        # Check USA
        usa_mismatches = db.query(Business.id, Business.state, Business.address).filter(
            func.upper(Business.country) == 'USA'
        ).all()
        
        usa_bad = []
        from ingest_csvs import US_STATE_MAP
        for rid, state, addr in usa_mismatches:
            # If address contains a different state name
            for abbr, name in US_STATE_MAP.items():
                if name.lower() in addr.lower() and name.lower() != state.lower():
                    usa_bad.append((rid, state, name, addr))
                    break
        
        print(f"Potential USA mismatches: {len(usa_bad)}")
        for rid, state, found, addr in usa_bad[:10]:
            print(f"ID: {rid} | State: {state} | Found in Addr: {found} | Addr: {addr[:100]}...")

        # Check UAE
        uae_mismatches = db.query(Business.id, Business.state, Business.address).filter(
            func.upper(Business.country) == 'UAE'
        ).all()
        
        uae_states = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah", "Umm Al Quwain", "Fujairah"]
        uae_bad = []
        for rid, state, addr in uae_mismatches:
            for s in uae_states:
                if s.lower() in addr.lower() and s.lower() != state.lower():
                    uae_bad.append((rid, state, s, addr))
                    break
                    
        print(f"\nPotential UAE mismatches: {len(uae_bad)}")
        for rid, state, found, addr in uae_bad[:10]:
            print(f"ID: {rid} | State: {state} | Found in Addr: {found} | Addr: {addr[:100]}...")
            
    finally:
        db.close()

if __name__ == "__main__":
    check_mislabeled_all()
