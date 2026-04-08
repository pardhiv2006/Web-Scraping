import sqlite3

def find_dashes():
    conn = sqlite3.connect('businesses.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM businesses")
    rows = cursor.fetchall()
    
    count = 0
    for row in rows:
        row_invalid = False
        reasons = []
        for col in row.keys():
            val = row[col]
            if val is not None:
                val_str = str(val).strip()
                if val_str == '-' or 'None' in val_str or 'n/a' in val_str.lower():
                    row_invalid = True
                    reasons.append(f"{col}='{val_str}'")
        if row_invalid:
            print(f"Row {row['id']}: {', '.join(reasons)}")
            count += 1
            if count > 20: break
            
    print(f"Found {count} rows with weird dashes or N/A in the first scan.")

if __name__ == '__main__':
    find_dashes()
