
import sqlite3
import os
import random
from datetime import datetime, timedelta

def strict_cleanup():
    # Target the root businesses.db explicitly
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(backend_dir)
    db_path = os.path.join(project_root, 'businesses.db')
    
    if not os.path.exists(db_path):
        # Fallback to backend dir if not at root
        db_path = os.path.join(backend_dir, 'businesses.db')
    
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return

    print(f"Cleaning database at: {db_path}")
    
    # Create backup before modification
    backup_path = f"{db_path}.pre_comprehensive_cleanup.bak"
    import shutil
    shutil.copy2(db_path, backup_path)
    print(f"Backup created at: {backup_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get column names
    cursor.execute("PRAGMA table_info(businesses)")
    columns = [col[1] for col in cursor.fetchall()]

    # Fetch all records
    cursor.execute("SELECT * FROM businesses")
    rows = cursor.fetchall()

    print(f"Total records in database: {len(rows)}")

    # Specific date range: 2025-06-01 to 2026-04-07
    start_date = datetime(2025, 6, 1)
    end_date = datetime(2026, 4, 7)
    delta_days = (end_date - start_date).days

    deleted_count = 0
    updated_count = 0
    kept_count = 0

    def is_blank(val):
        if val is None:
            return True
        s = str(val).strip().lower()
        return s in ["", "none", "-", "null", "undefined", "n/a", "-"] or not s

    for row in rows:
        biz_id = row[0]
        blanks = []
        for i, val in enumerate(row):
            col_name = columns[i]
            # Skip metadata columns
            if col_name in ['id', 'scraped_at', 'status']:
                continue
            if is_blank(val):
                blanks.append(col_name)

        if not blanks:
            kept_count += 1
            continue

        # Check if ONLY registration_date is blank
        if len(blanks) == 1 and blanks[0] == 'registration_date':
            # Fill with random date
            offset = random.randint(0, delta_days)
            new_date = (start_date + timedelta(days=offset)).strftime("%Y-%m-%d")
            cursor.execute("UPDATE businesses SET registration_date = ? WHERE id = ?", (new_date, biz_id))
            updated_count += 1
            print(f"[UPDATE] ID {biz_id}: Filled missing reg date with {new_date}")
        else:
            # Delete record
            cursor.execute("DELETE FROM businesses WHERE id = ?", (biz_id,))
            deleted_count += 1
            if deleted_count <= 20: # Only print first 20 for log clarity
                print(f"[DELETE] ID {biz_id}: {row[1]} | Blanks: {blanks[:3]}... ({len(blanks)} total)")

    conn.commit()
    conn.close()

    print("\n--- Comprehensive Cleanup Results ---")
    print(f"Records kept (no changes): {kept_count}")
    print(f"Records updated (reg date filled): {updated_count}")
    print(f"Records deleted (incomplete/Discovery): {deleted_count}")
    print(f"Final record count: {kept_count + updated_count}")

if __name__ == "__main__":
    strict_cleanup()
