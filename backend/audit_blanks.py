import sqlite3

EMPTY_STRINGS = {'', '-', '--', 'n/a', 'N/A', 'none', 'None', 'null', 'NULL',
                 'tbd', 'TBD', 'not available', 'unknown', 'na', 'NA', 'N/a'}

def is_blank(value):
    if value is None:
        return True
    if isinstance(value, str):
        s = value.strip()
        if not s or s in EMPTY_STRINGS:
            return True
    return False

def audit():
    conn = sqlite3.connect('backend/businesses.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM businesses")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    
    critical_fields = [
        'company_name', 'registration_number', 'registration_date', 'status', 
        'address', 'country', 'state', 'email', 'phone', 'website', 
        'linkedin_url', 'ceo_name', 'founder_name', 'ceo_email',
        'industry', 'employee_count', 'revenue', 'description'
    ]
    
    incomplete_ids = []
    field_stats = {f: 0 for f in critical_fields}
    
    for row in rows:
        record = dict(zip(columns, row))
        missing_in_record = []
        for f in critical_fields:
            if is_blank(record.get(f)):
                missing_in_record.append(f)
                field_stats[f] += 1
        
        if missing_in_record:
            incomplete_ids.append((record['id'], record['company_name'], missing_in_record))
            
    print(f"Total Records: {len(rows)}")
    print(f"Incomplete Records: {len(incomplete_ids)}")
    print("\nMissing Values per Field:")
    for f, count in field_stats.items():
        print(f"  {f:15}: {count:5}")
        
    if incomplete_ids:
        print("\nSample Incomplete Records (first 10):")
        for rid, name, fields in incomplete_ids[:10]:
            print(f"  ID:{rid:4} | {name:30} | Missing: {', '.join(fields)}")

if __name__ == "__main__":
    audit()
