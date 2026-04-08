import sqlite3
import os

DB_PATH = 'businesses.db'

def verify():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Define fields to check
    fields = [
        'email', 'phone', 'website', 'ceo_name', 
        'linkedin_url', 'industry', 'employee_count', 
        'revenue', 'description'
    ]

    print("--- Business Data Completeness Report ---")
    
    # Check total records
    cursor.execute("SELECT COUNT(*) FROM businesses")
    total = cursor.fetchone()[0]
    print(f"Total Records: {total}")

    # Check records with at least one blank field
    query = " OR ".join([f"({f} IS NULL OR {f} = '')" for f in fields])
    cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE {query}")
    incomplete = cursor.fetchone()[0]
    print(f"Incomplete Records: {incomplete} ({ (incomplete/total)*100:.1f}%)")

    # Check breakdown by field
    print("\nMissing Values per Field:")
    for f in fields:
        cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE {f} IS NULL OR {f} = ''")
        missing = cursor.fetchone()[0]
        print(f"  {f:15}: {missing:5} ({ (missing/total)*100:.1f}%)")

    # Detailed check for recent IDs (>= 1650)
    print("\nRecent Status (ID >= 1650):")
    cursor.execute(f"SELECT id, company_name FROM businesses WHERE id >= 1650 ORDER BY id DESC")
    recent_total = cursor.rowcount if cursor.rowcount != -1 else 0 # rowcount doesn't work for SELECT always in sqlite
    recent_rows = cursor.fetchall()
    
    for row_id, company in recent_rows:
        cursor.execute(f"SELECT " + ", ".join(fields) + f" FROM businesses WHERE id = {row_id}")
        data = cursor.fetchone()
        missing_in_row = [fields[i] for i, val in enumerate(data) if not val or val == '']
        status = "✓ COMPLETE" if not missing_in_row else f"✗ MISSING: {', '.join(missing_in_row)}"
        print(f"  ID:{row_id:4} | {company[:20]:20} | {status}")

    conn.close()

if __name__ == "__main__":
    verify()
