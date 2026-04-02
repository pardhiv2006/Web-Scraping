import sys
from datetime import datetime, timedelta
from database import SessionLocal
from models.business import Business
from sqlalchemy.orm import Session

def verify_data():
    db: Session = SessionLocal()
    try:
        businesses = db.query(Business).all()
        print(f"Verifying {len(businesses)} records...")
        
        # 1. Registration Date Uniqueness per State
        groups = {}
        for b in businesses:
            key = (b.country, b.state)
            if key not in groups:
                groups[key] = []
            groups[key].append(b)
            
        for (country, state), group in groups.items():
            dates = [b.registration_date for b in group]
            unique_dates = set(dates)
            if len(dates) != len(unique_dates):
                print(f"FAILED: Non-unique dates in {country}|{state}")
                # Print duplicates for debugging
                seen = set()
                for d in dates:
                    if d in seen:
                        print(f"  Duplicate date: {d}")
                    seen.add(d)
            
            # 2. Non-consecutive dates for consecutive records
            for i in range(len(group) - 1):
                d1 = datetime.strptime(group[i].registration_date, "%Y-%m-%d")
                d2 = datetime.strptime(group[i+1].registration_date, "%Y-%m-%d")
                if abs((d1 - d2).days) == 1:
                    print(f"FAILED: Consecutive dates for consecutive records in {country}|{state} at indices {i}, {i+1}")
                    print(f"  {group[i].registration_date} and {group[i+1].registration_date}")

        # 3. Phone Uniqueness Globally
        phones = [b.phone for b in businesses]
        unique_phones = set(phones)
        if len(phones) != len(unique_phones):
            print(f"FAILED: Non-unique phone numbers globally. Total: {len(phones)}, Unique: {len(unique_phones)}")
            
        # 4. Date Range Check
        start_date = datetime(2025, 1, 1)
        end_date = datetime.now()
        for b in businesses:
            d = datetime.strptime(b.registration_date, "%Y-%m-%d")
            if d < start_date or d > end_date:
                print(f"FAILED: Date out of range: {b.registration_date} for {b.company_name}")

        # 5. Phone Format and ISD Code Check
        for b in businesses:
            if b.country == "US":
                if not b.phone.startswith("+1"):
                    print(f"FAILED: Incorrect ISD for US: {b.phone}")
            elif b.country == "UK":
                if not b.phone.startswith("+44"):
                    print(f"FAILED: Incorrect ISD for UK: {b.phone}")
            elif b.country == "UAE":
                if not b.phone.startswith("+971"):
                    print(f"FAILED: Incorrect ISD for UAE: {b.phone}")

        print("Verification complete.")
        
    finally:
        db.close()

if __name__ == "__main__":
    verify_data()
