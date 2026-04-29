import sqlite3

def audit_database():
    conn = sqlite3.connect('businesses.db')
    cursor = conn.cursor()
    
    # Get all column names from businesses table
    cursor.execute("PRAGMA table_info(businesses)")
    columns = [row[1] for row in cursor.fetchall()]
    
    placeholders = ["-", "Not Available", "N/A", "Unknown", "null", "undefined", "None"]
    
    total_records = cursor.execute("SELECT COUNT(*) FROM businesses").fetchone()[0]
    print(f"Total Records: {total_records}")
    
    for col in columns:
        # Count NULLs and empty strings
        cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE {col} IS NULL OR trim({col}) = ''")
        blank_count = cursor.fetchone()[0]
        
        # Count specific placeholders
        placeholder_counts = {}
        total_placeholders = 0
        for p in placeholders:
            cursor.execute(f"SELECT COUNT(*) FROM businesses WHERE lower(trim({col})) = ?", (p.lower(),))
            count = cursor.fetchone()[0]
            if count > 0:
                placeholder_counts[p] = count
                total_placeholders += count
        
        if blank_count > 0 or total_placeholders > 0:
            print(f"\nColumn: {col}")
            print(f"  Blanks/NULLs: {blank_count}")
            for p, count in placeholder_counts.items():
                print(f"  '{p}': {count}")
            print(f"  Total Incomplete: {blank_count + total_placeholders}")

    conn.close()

if __name__ == "__main__":
    audit_database()
