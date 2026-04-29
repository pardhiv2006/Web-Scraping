import sqlite3
import json

def identify_missing_data():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    placeholders = {'n/a', 'unknown', 'not found', 'placeholder', 'none', 'null', 'not available', '-', 'undefined'}
    
    def is_blank(val):
        if val is None:
            return True
        s_val = str(val).strip().lower()
        if s_val == "" or s_val in placeholders:
            return True
        return False

    cursor.execute("SELECT * FROM businesses")
    rows = cursor.fetchall()
    
    missing_data = []
    
    for row in rows:
        missing_fields = []
        for col in row.keys():
            if col in ['id', 'scraped_at']:
                continue
            if is_blank(row[col]):
                missing_fields.append(col)
        
        if missing_fields:
            missing_data.append({
                "id": row['id'],
                "company_name": row['company_name'],
                "country": row['country'],
                "state": row['state'],
                "missing_fields": missing_fields,
                "current_data": {col: row[col] for col in row.keys() if col not in ['id', 'scraped_at']}
            })
            
    with open('records_to_fix.json', 'w') as f:
        json.dump(missing_data, f, indent=2)
    
    print(f"Identified {len(missing_data)} records to fix. Saved to records_to_fix.json")

if __name__ == "__main__":
    identify_missing_data()
