import sqlite3
import re
import os

DB_PATH = "businesses.db"

def clean_address(addr):
    if not addr:
        return addr
    
    # Check for JSON array at the beginning: [{"country":"..."...}]
    # We want to strip the part that looks like a JSON array.
    # Example: [{"country":"gb","locations":1}], london, UK
    
    # Try to find the end of the JSON array if it starts with [
    if addr.startswith('['):
        # Find the closing ] followed by optionally a comma or space
        # We'll use a regex to find the first ] that is followed by the rest of the address
        match = re.match(r'^\[.*?\]\s*,?\s*(.*)$', addr)
        if match:
            cleaned = match.group(1).strip()
            # If after stripping we get something like ", London, UK", strip the leading comma
            cleaned = re.sub(r'^,\s*', '', cleaned)
            if cleaned:
                return cleaned
    
    return addr

def run_cleanup():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT id, address FROM businesses WHERE address LIKE '[%';")
    rows = cursor.fetchall()
    
    print(f"🔍 Found {len(rows)} records with potential JSON addresses.")
    
    updates = 0
    for row_id, old_address in rows:
        new_address = clean_address(old_address)
        if new_address != old_address:
            cursor.execute("UPDATE businesses SET address = ? WHERE id = ?", (new_address, row_id))
            updates += 1
            if updates % 100 == 0:
                print(f"✅ Cleaned {updates} addresses...")

    conn.commit()
    conn.close()
    print(f"✨ Cleanup complete. Total addresses updated: {updates}")

if __name__ == "__main__":
    run_cleanup()
