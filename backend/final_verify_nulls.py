
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'businesses.db')

def verify():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Define all fields to check for ANY blank/null values
    fields = [
        'company_name', 'registration_number', 'country', 'city', 'state', 
        'registration_date', 'address', 'status', 'email', 'phone', 
        'website', 'ceo_name', 'ceo_email', 'founder_name', 
        'linkedin_url', 'industry', 'employee_count', 'revenue', 'description'
    ]

    print("--- Final Data Completeness Audit ---")
    
    cursor.execute("SELECT COUNT(*) FROM businesses")
    total = cursor.fetchone()[0]
    print(f"Total Records: {total}")

    all_complete = True
    for f in fields:
        cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE {f} IS NULL OR {f} = '' OR {f} = '-' OR {f} = 'N/A'")
        missing = cursor.fetchone()[0]
        if missing > 0:
            all_complete = False
            print(f"  [!] {f:20}: {missing:5} blanks found ({(missing/total)*100:.1f}%)")
        else:
            print(f"  [✓] {f:20}: 0 blanks")

    if all_complete:
        print("\n✅ SUCCESS: All fields are 100% complete with no blanks or placeholder dashes.")
    else:
        print("\n❌ FAILED: Some fields still contain blank or placeholder values.")

    conn.close()

if __name__ == "__main__":
    verify()
