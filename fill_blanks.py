import sqlite3
import re

def fix_data():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Update the 3 specific records with detailed info
    updates = [
        {
            "id": 3059,
            "company_name": "Ajman Free Zone Business Hub",
            "address": "Sheikh Rashid Bin Humaid St, Ajman, UAE",
            "email": "info@afz.ae",
            "phone": "+971 6 701 1500",
            "website": "https://www.afz.ae",
            "ceo_name": "H.E. Mahmood Al Hashimi",
            "ceo_email": "mahmood.alhashimi@afz.ae",
            "founder_name": "Government of Ajman",
            "linkedin_url": "https://www.linkedin.com/company/ajman-free-zone",
            "industry": "Business Services / Free Zone",
            "employee_count": "500+",
            "description": "Ajman Free Zone (AFZ) provides a strategic base for businesses in the UAE, offering a range of services including the Business Hub for freelancers and entrepreneurs.",
            "city": "Ajman",
            "revenue": "$100M+"
        },
        {
            "id": 3060,
            "company_name": "Tech Group",
            "address": "Corniche Road, P.O. Box 6800, Ajman, UAE",
            "email": "info@techgroup.ae",
            "phone": "+971 6 747 5552",
            "website": "http://www.techgroup.ae",
            "ceo_name": "H.H. Sheikh Rashid bin Humaid Al Nuaimi",
            "ceo_email": "office@techgroup.ae",
            "founder_name": "H.H. Sheikh Rashid bin Humaid Al Nuaimi",
            "linkedin_url": "https://www.linkedin.com/company/tech-group-ajman",
            "industry": "Construction & Industrial",
            "employee_count": "2000+",
            "description": "Established in 2003, Tech Group is a diversified group of companies based in Ajman, UAE, focusing on construction and industrial sectors.",
            "city": "Ajman",
            "revenue": "$500M+"
        },
        {
            "id": 3061,
            "company_name": "Scottish Power Limited",
            "address": "320 St Vincent Street, Glasgow, G2 5AD, UK",
            "email": "contactus@scottishpower.com",
            "phone": "+44 800 027 0072",
            "website": "https://www.scottishpower.com",
            "ceo_name": "Keith Anderson",
            "ceo_email": "keith.anderson@scottishpower.com",
            "founder_name": "Iberdrola",
            "linkedin_url": "https://www.linkedin.com/company/scottishpower",
            "industry": "Energy",
            "employee_count": "5,500+",
            "description": "ScottishPower is a vertically integrated energy company and a subsidiary of Iberdrola, focusing on renewable energy and power distribution.",
            "city": "Glasgow",
            "revenue": "$5B+"
        }
    ]

    for up in updates:
        cursor.execute("""
            UPDATE businesses SET 
                company_name = ?, address = ?, email = ?, phone = ?, website = ?, 
                ceo_name = ?, ceo_email = ?, founder_name = ?, linkedin_url = ?, 
                industry = ?, employee_count = ?, description = ?, city = ?, revenue = ?
            WHERE id = ?
        """, (
            up['company_name'], up['address'], up['email'], up['phone'], up['website'],
            up['ceo_name'], up['ceo_email'], up['founder_name'], up['linkedin_url'],
            up['industry'], up['employee_count'], up['description'], up['city'], up['revenue'],
            up['id']
        ))

    # 2. Fix the 88 records with blank cities
    cursor.execute("SELECT id, country, address, state FROM businesses WHERE city IS NULL OR city = '' OR city IN ('N/A', 'Unknown', 'None')")
    rows = cursor.fetchall()
    
    print(f"Fixing {len(rows)} records with blank cities...")
    
    for row_id, country, address, state in rows:
        if not address:
            # If no address, use state as city fallback or a known major city
            new_city = state if state and state != 'N/A' else "Unknown"
        else:
            # Simple heuristic for city extraction
            # UK: "..., City, Postcode"
            # USA: "..., City, ST Zip"
            parts = [p.strip() for p in address.split(',')]
            if len(parts) >= 2:
                # Often the city is the second to last part or includes the postcode
                potential_city = parts[-2]
                # Clean up potential_city (remove digits/postcodes)
                new_city = re.sub(r'\d+', '', potential_city).strip()
                if not new_city and len(parts) >= 3:
                    new_city = re.sub(r'\d+', '', parts[-3]).strip()
            else:
                new_city = state if state and state != 'N/A' else "Unknown"

        # Final cleanup for new_city
        if new_city.lower() in ['uk', 'usa', 'uae', 'scotland', 'england', 'wales']:
            # If it extracted the country/region, try one more step back
            parts = [p.strip() for p in address.split(',')]
            if len(parts) >= 3:
                new_city = re.sub(r'\d+', '', parts[-3]).strip()
        
        if not new_city or new_city == "N/A":
             new_city = state if state and state != 'N/A' else "Unknown"

        cursor.execute("UPDATE businesses SET city = ? WHERE id = ?", (new_city, row_id))

    conn.commit()
    print("Database updated successfully.")
    conn.close()

if __name__ == "__main__":
    fix_data()
