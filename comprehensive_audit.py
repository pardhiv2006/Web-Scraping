import sqlite3
from collections import defaultdict

def audit_blanks():
    db_path = "businesses.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM businesses")
        rows = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"Error accessing database: {e}")
        return
    
    if not rows:
        print("No records found in the database.")
        return

    columns = rows[0].keys()
    
    # Placeholders to consider as blank
    placeholders = {'n/a', 'unknown', 'not found', 'placeholder', 'none', 'null', 'not available', '-', 'undefined'}

    def is_blank(val):
        if val is None:
            return True
        s_val = str(val).strip().lower()
        if s_val == "" or s_val in placeholders:
            return True
        return False

    # summary structure: {(country, state): [total_count, blank_count]}
    summary = defaultdict(lambda: [0, 0])
    
    total_total = 0
    total_blanks = 0

    for row in rows:
        country = row['country'] if row['country'] else "Unknown"
        state = row['state'] if row['state'] else "N/A"
        
        has_blank = False
        for col in columns:
            if col in ['id', 'scraped_at']: 
                continue
            if is_blank(row[col]):
                has_blank = True
                break
        
        summary[(country, state)][0] += 1
        total_total += 1
        if has_blank:
            summary[(country, state)][1] += 1
            total_blanks += 1

    print(f"{'Country':<15} | {'State':<15} | {'Total Records':<15} | {'With Blanks':<15}")
    print("-" * 68)
    
    # Sort summary by country and state
    sorted_keys = sorted(summary.keys())
    for key in sorted_keys:
        country, state = key
        total, blanks = summary[key]
        print(f"{country:<15} | {state:<15} | {total:<15} | {blanks:<15}")

    print("\n--- Final Totals ---")
    print(f"Total Records Verified: {total_total}")
    print(f"Total Records with at least one blank field: {total_blanks}")
    if total_total > 0:
        print(f"Percentage of records with blanks: {(total_blanks/total_total)*100:.2f}%")

if __name__ == "__main__":
    audit_blanks()
