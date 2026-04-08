
import sqlite3
import os

DB_PATH = 'businesses.db'

def audit():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT id, registration_date FROM businesses ORDER BY id ASC")
    rows = cursor.fetchall()
    
    consecutive_matches = 0
    range_errors = 0
    start_dt = "2025-06-01"
    end_dt = "2026-04-07"
    
    for i in range(1, len(rows)):
        # Check consecutive
        if rows[i][1] == rows[i-1][1]:
            consecutive_matches += 1
            
    for row in rows:
        # Check range
        if not row[1] or not (start_dt <= row[1] <= end_dt):
            range_errors += 1
            
    print(f"--- Registration Date Audit ---")
    print(f"Total Records checked: {len(rows)}")
    print(f"Consecutive Date Matches (Errors): {consecutive_matches}")
    print(f"Date Range Errors (out of 2025-06-01 to 2026-04-07): {range_errors}")
    
    if consecutive_matches == 0 and range_errors == 0:
        print("\n✅ SUCCESS: All dates are within range and no consecutive records share the same date.")
    else:
        print("\n❌ FAILED: Found issues in date distribution.")

    conn.close()

if __name__ == "__main__":
    audit()
