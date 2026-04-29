
import os
import sys

# Add backend to sys.path
backend_path = os.path.join(os.getcwd(), "backend")
sys.path.insert(0, backend_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func

def find_hidden_england():
    db = SessionLocal()
    try:
        # Check all UK records
        results = db.query(Business.state, func.count(Business.id)).filter(
            func.upper(Business.country) == 'UK'
        ).group_by(Business.state).all()
        
        print("UK records by state:")
        for state, count in results:
            print(f"'{state}': {count}")
            
        # Check for address containing England but state is not England
        others = db.query(Business.state, Business.address).filter(
            func.upper(Business.country) == 'UK',
            func.upper(Business.state) != 'ENGLAND',
            Business.address.like('%England%')
        ).all()
        
        print(f"\nUK records with 'England' in address but state is NOT England: {len(others)}")
        for state, addr in others:
            print(f"State: '{state}', Address: {addr[:50]}...")
            
    finally:
        db.close()

if __name__ == "__main__":
    find_hidden_england()
