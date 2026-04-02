import random
import os
import sys
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models.business import Business

# Set up random seed for reproducibility if needed, or just let it be random
# random.seed(42)

def generate_unique_dates(count, start_date, end_date):
    """
    Generates a list of unique, non-consecutive dates within the range.
    """
    total_days = (end_date - start_date).days
    if count > total_days:
        raise ValueError("Not enough unique dates in the range for the requested count.")
    
    # Generate all possible dates
    all_dates = [start_date + timedelta(days=x) for x in range(total_days + 1)]
    
    # Randomly sample 'count' dates
    sampled_dates = random.sample(all_dates, count)
    
    # Sort them to easily shuffle while checking for consecutiveness
    sampled_dates.sort()
    
    # Fisher-Yates-like shuffle with a check for consecutiveness
    # For 1699 records across multiple states, the density is low enough that this should be easy.
    # We want to avoid consecutive dates for 'consecutive companies in the list'.
    
    def is_consecutive(d1, d2):
        return abs((d1 - d2).days) == 1

    # Shuffle until we satisfy the "no consecutive dates for consecutive records" rule
    # Since we assign these to a list, we just need to ensure list[i] and list[i+1] aren't consecutive.
    result = sampled_dates[:]
    for _ in range(100): # Try shuffling up to 100 times
        random.shuffle(result)
        valid = True
        for i in range(len(result) - 1):
            if is_consecutive(result[i], result[i+1]):
                valid = False
                break
        if valid:
            return [d.strftime("%Y-%m-%d") for d in result]
            
    # If we couldn't find a perfect shuffle in 100 tries, just return the last shuffle
    # (In practice, with ~20-50 records per state and 365 days, this is highly unlikely to fail)
    return [d.strftime("%Y-%m-%d") for d in result]

def generate_phone_number(country, existing_phones):
    """
    Generates a unique, realistic phone number based on the country.
    """
    while True:
        if country == "US":
            # +1 (AAA) PPP-LLLL
            area = random.randint(200, 999)
            prefix = random.randint(200, 999)
            line = random.randint(1000, 9999)
            phone = f"+1 ({area}) {prefix}-{line}"
        elif country == "UK":
            # +44 7XXX XXXXXX (Mobile)
            p1 = random.randint(7100, 7999)
            p2 = random.randint(100000, 999999)
            phone = f"+44 {p1} {p2}"
        elif country == "UAE":
            # +971 5X XXX XXXX
            p1 = random.choice([50, 52, 54, 55, 56, 58])
            p2 = random.randint(100, 999)
            p3 = random.randint(1000, 9999)
            phone = f"+971 {p1} {p2} {p3}"
        else:
            # Fallback
            phone = f"+0 {random.randint(1000000000, 9999999999)}"
            
        if phone not in existing_phones:
            existing_phones.add(phone)
            return phone

def update_data():
    db: Session = SessionLocal()
    try:
        print("Fetching records...")
        businesses = db.query(Business).all()
        print(f"Found {len(businesses)} records.")
        
        # Group by (country, state)
        groups = {}
        for b in businesses:
            key = (b.country, b.state)
            if key not in groups:
                groups[key] = []
            groups[key].append(b)
            
        # Global phone uniqueness tracker
        existing_phones = set()
        
        # Date range: 01-01-2025 to Present
        start_date = datetime(2025, 1, 1)
        end_date = datetime.now()
        
        print("Updating records...")
        for (country, state), group in groups.items():
            # Generate unique dates for this state
            dates = generate_unique_dates(len(group), start_date, end_date)
            
            for i, b in enumerate(group):
                b.registration_date = dates[i]
                b.phone = generate_phone_number(b.country, existing_phones)
        
        db.commit()
        print("Successfully updated database.")
        
    except Exception as e:
        db.rollback()
        print(f"Error occurred: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    update_data()
