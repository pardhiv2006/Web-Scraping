import sqlite3

def update_batch_3():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    updates = [
        {
            "id": 1790,
            "ceo_name": "Claire Miles",
            "ceo_email": "claire.miles@stagecoachgroup.com",
            "employee_count": "23,000",
            "revenue": "£1.3 Billion",
            "linkedin_url": "https://www.linkedin.com/company/stagecoach-group-plc",
            "description": "Stagecoach Group is a leading multi-modal UK public transport company, operating bus, coach, and tram services across the country."
        },
        {
            "id": 1796,
            "ceo_name": "Gregory Case",
            "ceo_email": "gregory.case@aon.com",
            "employee_count": "50,000",
            "revenue": "$17.03 Billion",
            "linkedin_url": "https://www.linkedin.com/company/aon",
            "description": "Aon plc is a leading global professional services firm providing a broad range of risk, retirement, and health solutions."
        },
        {
            "id": 1800,
            "ceo_name": "Robert A. Michael",
            "ceo_email": "robert.michael@abbvie.com",
            "employee_count": "57,000",
            "revenue": "$61.2 Billion",
            "linkedin_url": "https://www.linkedin.com/company/abbvie",
            "description": "AbbVie Inc. is a global, research-driven biopharmaceutical company committed to developing innovative therapies that address some of the world's most complex diseases."
        },
        {
            "id": 1794,
            "ceo_name": "Wendell P. Weeks",
            "ceo_email": "wendell.weeks@corning.com",
            "employee_count": "67,200",
            "revenue": "$15.63 Billion",
            "linkedin_url": "https://www.linkedin.com/company/corning-incorporated",
            "description": "Corning Incorporated is one of the world's leading innovators in materials science, specializing in glass, ceramics, and optical physics."
        },
        {
            "id": 2094,
            "ceo_name": "Yahya Taher",
            "ceo_email": "yahya.taher@multibankfx.com",
            "revenue": "$306 Million",
            "linkedin_url": "https://www.linkedin.com/company/multibank-group",
            "description": "MultiBank Group is one of the largest financial derivatives providers worldwide, offering award-winning trading platforms and tight spreads."
        },
        {
            "id": 2089,
            "ceo_name": "Paul O'Flaherty",
            "ceo_email": "paul.oflaherty@alnaboodah.com",
            "employee_count": "10,000",
            "linkedin_url": "https://www.linkedin.com/company/al-naboodah",
            "description": "The Al Naboodah Group is a leading family-owned conglomerate in the UAE, with a diverse portfolio across construction, commercial, and investment sectors."
        },
        {
            "id": 1784,
            "ceo_name": "HE Masaood Ahmed Al Masaood",
            "ceo_email": "masaood.almasaood@masaood.com",
            "linkedin_url": "https://www.linkedin.com/company/al-masaood",
            "description": "Al Masaood is a privately held family-owned industrial group based in Abu Dhabi, representing a wide range of global brands and industries."
        }
    ]
    
    for up in updates:
        fields = []
        values = []
        for k, v in up.items():
            if k == 'id': continue
            fields.append(f"{k} = ?")
            values.append(v)
        
        if not fields: continue
        
        values.append(up['id'])
        query = f"UPDATE businesses SET {', '.join(fields)} WHERE id = ?"
        cursor.execute(query, tuple(values))
    
    conn.commit()
    conn.close()
    print("Batch 3 updated successfully.")

if __name__ == "__main__":
    update_batch_3()
