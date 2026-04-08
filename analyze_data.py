import sqlite3

def analyze_more():
    conn = sqlite3.connect('businesses.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM businesses")
    rows = cursor.fetchall()
    
    placeholders = ['n/a', 'na', 'none', 'null', 'nan', '-', 'unknown', 'tbd', 'not available', 'empty', 'blank', 'tba', 'test', 'demo']
    
    invalid_rows_ids = []
    
    for row in rows:
        row_invalid = False
        reasons = []
        for col in row.keys():
            val = row[col]
            if val is None:
                row_invalid = True
                reasons.append(f"{col} IS_NULL")
                continue
                
            val_str = str(val).strip()
            val_lower = val_str.lower()
            
            if val_str == '':
                row_invalid = True
                reasons.append(f"{col} IS_EMPTY")
            elif val_lower in placeholders:
                row_invalid = True
                reasons.append(f"{col} IS_PLACEHOLDER ({val_str})")
            elif '-' in val_str and any(c.isdigit() for c in val_str) and col in ['employee_count', 'revenue'] and val_lower != 'e-commerce':
                # what if revenue is "1M-10M"? Or dates? "registration_date" has '-' typically (YYYY-MM-DD).
                # Only check employee_count and revenue for dash ranges (if user wants no ranges like 1-10)
                row_invalid = True
                reasons.append(f"{col} IS_RANGE ({val_str})")
            elif col != 'id' and val_str in ['0', '00', '123', '12345', '123456789', '9999999999', '0000000000']:
                row_invalid = True
                reasons.append(f"{col} IS_FAKE_NUM ({val_str})")
            elif col in ['industry', 'ceo_name', 'founder_name', 'company_name'] and val_str.isdigit():
                row_invalid = True
                reasons.append(f"{col} IS_ONLY_NUMBERS ({val_str})")

        if row_invalid:
            invalid_rows_ids.append((row['id'], reasons))

    print(f"Total invalid rows: {len(invalid_rows_ids)} out of {len(rows)}")
    for i, (rid, reasons) in enumerate(invalid_rows_ids[:20]):
        print(f"Row {rid}: {', '.join(reasons)}")
        
    print("\nLooking at typical data (first valid row):")
    for row in rows:
        if row['id'] not in [x[0] for x in invalid_rows_ids]:
            print(dict(row))
            break

if __name__ == '__main__':
    analyze_more()
