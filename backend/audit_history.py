import sqlite3
import json

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
    cursor.execute("SELECT id, country, states, result_count, result_data FROM search_history")
    rows = cursor.fetchall()
    
    fields_to_check = ['email', 'phone', 'website', 'ceo_name', 'ceo_email', 'industry']
    
    stale_history = []
    
    for hid, country, states, count, data_json in rows:
        if not data_json:
            continue
            
        try:
            records = json.loads(data_json)
        except:
            continue
            
        blanks_found = 0
        for b in records:
            if any(is_blank(b.get(f)) for f in fields_to_check):
                blanks_found += 1
        
        if blanks_found > 0:
            stale_history.append((hid, country, states, blanks_found, len(records)))
            
    print(f"Total History Items: {len(rows)}")
    print(f"Stale History Items (containing blanks): {len(stale_history)}")
    
    if stale_history:
        print("\nSample Stale History Items:")
        for hid, country, states, blanks, total in stale_history[:10]:
            print(f"  ID:{hid:4} | {country:10} | {states:20} | {blanks}/{total} records have blanks")

if __name__ == "__main__":
    audit()
