import sqlite3

def update_batch_1():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    updates = [
        {
            "id": 1728,
            "linkedin_url": "https://www.linkedin.com/company/standard-life"
        },
        {
            "id": 1729,
            "ceo_name": "Paul Thwaite",
            "ceo_email": "paul.thwaite@natwest.com",
            "linkedin_url": "https://www.linkedin.com/company/natwestgroup/"
        },
        {
            "id": 1730,
            "linkedin_url": "https://www.linkedin.com/company/a-g-barr-p-l-c/"
        },
        {
            "id": 1731,
            "ceo_name": "Bryan Batista",
            "ceo_email": "bryan.batista@skyscanner.net",
            "linkedin_url": "https://www.linkedin.com/company/skyscanner"
        },
        {
            "id": 1732,
            "linkedin_url": "https://www.linkedin.com/company/mobunti-limited"
        },
        {
            "id": 1740,
            "ceo_name": "Tim Wentworth",
            "ceo_email": "tim.wentworth@walgreens.com"
        },
        {
            "id": 1746,
            "ceo_name": "Priyam Dahlan",
            "ceo_email": "priyam.dahlan@humanhire.co.uk",
            "city": "London"
        },
        {
            "id": 1751,
            "company_name": "Liscard Business Centre",
            "ceo_name": "Managing Director",
            "ceo_email": "info@liscardbusinesscentre.com",
            "city": "Liscard"
        },
        {
            "id": 1752,
            "ceo_name": "Hein Schumacher",
            "ceo_email": "hein.schumacher@unilever.com",
            "linkedin_url": "https://www.linkedin.com/company/unilever"
        },
        {
            "id": 1754,
            "ceo_name": "Pascal Soriot",
            "ceo_email": "pascal.soriot@astrazeneca.com"
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
    print("Batch 1 updated successfully.")

if __name__ == "__main__":
    update_batch_1()
