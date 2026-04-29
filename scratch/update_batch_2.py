import sqlite3

def update_batch_2():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    updates = [
        {"id": 1755, "ceo_name": "Robert DeNicola", "ceo_email": "robert.denicola@phoenixrefreshmentgroup.com", "linkedin_url": "https://www.linkedin.com/company/phoenix-refreshment-group"},
        {"id": 1757, "ceo_name": "Shekhar Iyer", "ceo_email": "shekhar.iyer@arangodb.com"},
        {"id": 1758, "ceo_name": "Henrik Fisker", "ceo_email": "hfisker@fiskerinc.com"},
        {"id": 1760, "ceo_name": "Hock Tan", "ceo_email": "hock.tan@broadcom.com"},
        {"id": 1761, "ceo_name": "Sundar Pichai", "ceo_email": "sundar@google.com"},
        {"id": 1762, "linkedin_url": "https://www.linkedin.com/company/admiral-group"},
        {"id": 1763, "linkedin_url": "https://www.linkedin.com/company/airbus"},
        {"id": 1764, "linkedin_url": "https://www.linkedin.com/company/dwr-cymru-welsh-water"},
        {"id": 1765, "linkedin_url": "https://www.linkedin.com/company/tatasteeluk"},
        {"id": 1766, "linkedin_url": "https://www.linkedin.com/company/7-steel-uk"},
        {"id": 1767, "ceo_name": "Darren Woods", "ceo_email": "darren.woods@exxonmobil.com"},
        {"id": 1768, "ceo_name": "John Stankey", "ceo_email": "john.stankey@att.com"},
        {"id": 1769, "ceo_name": "Bob Jordan", "ceo_email": "bob.jordan@wnco.com"},
        {"id": 1770, "ceo_name": "Michael Dell", "ceo_email": "michael@dell.com"},
        {"id": 1772, "ceo_name": "Tim Cook", "ceo_email": "tcook@apple.com"}
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
    print("Batch 2 updated successfully.")

if __name__ == "__main__":
    update_batch_2()
