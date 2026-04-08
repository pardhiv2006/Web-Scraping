import sys
import os
import re

# Set up paths
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)

from database import SessionLocal
from models.business import Business
from services.smart_scraper import _ai_research

def clean_junk_names():
    db = SessionLocal()
    junk_names = [
        "UnitedKingdom", "Service", "Explore", "The No", "Contact", "About",
        "Directory", "Companies", "Information", "Website", "Welcome",
        "Business", "Business Directory", "Company", "Services", "Search"
    ]
    junk_lower = [j.lower() for j in junk_names]
    
    try:
        deleted = 0
        businesses = db.query(Business).all()
        for b in businesses:
            if b.company_name and b.company_name.strip().lower() in junk_lower:
                print(f"Deleting junk company: {b.company_name}")
                db.delete(b)
                deleted += 1
        db.commit()
        print(f"Deleted {deleted} junk companies.")
    except Exception as e:
        print(f"Error deleting: {e}")
        db.rollback()
    finally:
        db.close()

def convert_currency_to_usd_ai(revenue_str, company_name):
    """Fallback if we just want to let AI format it as USD"""
    if not revenue_str or revenue_str == "-":
        return None
    
    # If it's already USD
    if "USD" in revenue_str.upper() or "$" in revenue_str:
        return revenue_str
        
    try:
        import g4f
        prompt = (
            f"Convert the following revenue string '{revenue_str}' (for company '{company_name}') into USD. "
            "Return ONLY the converted revenue string starting with a '$', e.g. '$10M', '$1.5B'. No explanation."
        )
        response = g4f.ChatCompletion.create(
            model=g4f.models.default,
            messages=[{"role": "user", "content": prompt}],
        )
        clean = response.strip("`'\n ")
        if "$" in clean:
            return clean
    except Exception as e:
        print(f"  AI convert error: {e}")
    return revenue_str

def backfill_and_normalize():
    db = SessionLocal()
    try:
        businesses = db.query(Business).filter(
            (Business.revenue == None) | (Business.revenue == "") | (Business.revenue == "-") |
            (Business.employee_count == None) | (Business.employee_count == "") | (Business.employee_count == "-") |
            (~Business.revenue.like("%$%")) # Not containing USD format
        ).all()
        
        print(f"Found {len(businesses)} companies needing revenue/employee backfill or currency normalization.")
        
        import g4f # Verify it's ready
        
        for b in businesses:
            updated = False
            
            # Currency normalization for existing revenue that isn't USD
            if b.revenue and b.revenue != "-" and "$" not in b.revenue:
                print(f"Normalizing currency for {b.company_name}: {b.revenue}")
                new_rev = convert_currency_to_usd_ai(b.revenue, b.company_name)
                if new_rev and new_rev != b.revenue:
                    b.revenue = new_rev
                    updated = True

            # If still missing
            missing_rev = not b.revenue or b.revenue == "-"
            missing_emp = not b.employee_count or b.employee_count == "-"
            
            if missing_rev or missing_emp:
                print(f"Backfilling using AI for: {b.company_name}")
                ai_data = _ai_research(
                    company_name=b.company_name, 
                    state=b.state or "", 
                    industry_hint=b.industry or "", 
                    country="US" # Force US format for USD
                )
                
                if missing_rev and ai_data.get("revenue"):
                    rev = ai_data["revenue"]
                    if "$" not in rev:
                        rev = convert_currency_to_usd_ai(rev, b.company_name)
                    b.revenue = rev
                    updated = True
                    
                if missing_emp and ai_data.get("employee_count"):
                    b.employee_count = ai_data["employee_count"]
                    updated = True
                    
            if updated:
                db.commit()
                print(f"  -> Updated {b.company_name}: Rev={b.revenue}, Emp={b.employee_count}")
                
    except Exception as e:
        print(f"Error backfilling: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    print("Starting DB Clean & Backfill...")
    clean_junk_names()
    backfill_and_normalize()
    print("Done.")
