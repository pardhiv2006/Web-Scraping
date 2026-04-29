import sqlite3
from collections import defaultdict

def get_blank_records():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM businesses")
    rows = cursor.fetchall()
    
    columns = rows[0].keys()
    placeholders = {'n/a', 'unknown', 'not found', 'placeholder', 'none', 'null'}

    def is_blank(val):
        if val is None:
            return True
        s_val = str(val).strip().lower()
        if s_val == "" or s_val in placeholders:
            return True
        return False

    blank_records = []
    field_blanks_count = defaultdict(int)

    for row in rows:
        blanks = []
        for col in columns:
            if col in ['id', 'scraped_at']: 
                continue
            if is_blank(row[col]):
                blanks.append(col)
                field_blanks_count[col] += 1
        
        if blanks:
            blank_records.append({
                'id': row['id'],
                'company_name': row['company_name'],
                'country': row['country'],
                'state': row['state'],
                'registration_number': row['registration_number'],
                'blank_fields': blanks
            })

    print(f"Total records with blanks: {len(blank_records)}")
    print("\nBlanks per field:")
    for field, count in sorted(field_blanks_count.items(), key=lambda x: x[1], reverse=True):
        print(f"  {field}: {count}")

    # Output details to a file for later processing
    with open('records_to_fix.json', 'w') as f:
        import json
        json.dump(blank_records, f, indent=2)

if __name__ == "__main__":
    get_blank_records()
