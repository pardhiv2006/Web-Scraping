
import os
import sys

# Add backend to sys.path
backend_path = os.path.join(os.getcwd(), "backend")
sys.path.insert(0, backend_path)

from database import SessionLocal
from models.business import Business
from sqlalchemy import func

def show_mislabeled_uk():
    db = SessionLocal()
    try:
        others = db.query(Business.id, Business.company_name, Business.state, Business.address).filter(
            func.upper(Business.country) == 'UK',
            func.upper(Business.state) != 'ENGLAND',
            Business.address.like('%England%')
        ).all()
        
        print(f"Mislabeled UK records:")
        for rid, name, state, addr in others:
            print(f"ID: {rid} | Name: {name} | Current State: {state} | Address: {addr}")
            
    finally:
        db.close()

if __name__ == "__main__":
    show_mislabeled_uk()
