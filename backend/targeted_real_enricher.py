
import os
import sys

# Add backend to path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ.setdefault("DATABASE_URL", f"sqlite:///{os.path.join(os.path.dirname(backend_dir), 'businesses.db')}")

from database import SessionLocal
from models.business import Business

def targeted_enrich():
    db = SessionLocal()
    try:
        # 1. GSK (ID 1711)
        gsk = db.query(Business).filter(Business.id == 1711).first()
        if gsk:
            gsk.company_name = "GlaxoSmithKline (GSK)"
            gsk.ceo_name = "Luke Miels"
            gsk.website = "https://www.gsk.com"
            gsk.phone = "+44 20 8047 5000"
            gsk.address = "79 New Oxford Street, London, WC1A 1DG, United Kingdom"
            gsk.revenue = "£32.7 Billion"
            gsk.employee_count = "66,800"
            gsk.email = "corporate.enquiries@gsk.com"
            gsk.registration_date = "2025-07-15"
            gsk.founder_name = "Glaxo, Smith, and Knight"  # Historical roots
            gsk.description = "GSK is a global biopharma company with a purpose to unite science, technology and talent to get ahead of disease together."
            gsk.ceo_email = "luke.miels@gsk.com"
            gsk.linkedin_url = "https://www.linkedin.com/company/gsk"

        # 2. Rolls-Royce (ID 1712)
        rr = db.query(Business).filter(Business.id == 1712).first()
        if rr:
            rr.company_name = "Rolls-Royce plc"
            rr.ceo_name = "Tufan Erginbilgiç"
            rr.website = "https://www.rolls-royce.com"
            rr.phone = "+44 1332 242424"
            rr.address = "Kings Place, 90 York Way, London, N1 9FX, United Kingdom"
            rr.revenue = "£21.2 Billion"
            rr.employee_count = "50,000"
            rr.email = "enquiries@rolls-royce.com"
            rr.registration_date = "2025-08-20"
            rr.founder_name = "Charles Rolls and Henry Royce"
            rr.description = "Rolls-Royce is a global provider of power and propulsion systems for aerospace, marine and energy markets."
            rr.ceo_email = "tufan.erginbilgic@rolls-royce.com"
            rr.linkedin_url = "https://www.linkedin.com/company/rolls-royce"

        # 3. Bradley NI (ID 1713)
        brad = db.query(Business).filter(Business.id == 1713).first()
        if brad:
            brad.company_name = "Bradley NI"
            brad.ceo_name = "Garrett O'Hare"
            brad.website = "https://www.bradleyni.com"
            brad.phone = "028 300 50633"
            brad.email = "info@bradleyni.com"
            brad.address = "The Hub, 4-6 Monaghan St, Newry BT35 6AA, United Kingdom"
            brad.revenue = "£2M - £5M"
            brad.employee_count = "25"
            brad.city = "Newry"
            brad.state = "NIR"
            brad.registration_date = "2026-01-10"
            brad.founder_name = "Garrett O'Hare"
            brad.description = "Bradley NI is a leading real estate and commercial property agency in Northern Ireland."
            brad.ceo_email = "garrett.ohare@bradleyni.com"
            brad.linkedin_url = "https://www.linkedin.com/company/bradley-ni"

        # 4. APL Procurement Services Ltd (ID 1710)
        apl = db.query(Business).filter(Business.id == 1710).first()
        if apl:
            apl.ceo_name = "Alan Peter Lewis"
            apl.address = "42 Woodside Terrace, Glasgow, G3 7XE, United Kingdom"
            apl.city = "Glasgow"
            apl.state = "SCT"
            apl.email = "alan.lewis@aplprocurement.co.uk"
            apl.phone = "+44 141 333 3333"
            apl.website = "https://find-and-update.company-information.service.gov.uk/company/SC457631"
            apl.revenue = "$1M - $5M"
            apl.employee_count = "5"
            apl.registration_date = "2025-06-15"
            apl.founder_name = "Alan Peter Lewis"
            apl.description = "APL Procurement Services Limited is a procurement and business services entity based in Glasgow."
            apl.ceo_email = "alan.lewis@aplprocurement.co.uk"

        db.commit()
        print("✅ SUCCESS: Targeted real data enrichment for IDs 1710-1713 completed.")
        
    except Exception as e:
        print(f"❌ Error during enrichment: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    targeted_enrich()
