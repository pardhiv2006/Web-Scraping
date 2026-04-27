import sqlite3
import random
from datetime import datetime, timedelta
import os

DB_PATH = "businesses.db"

def get_random_date():
    # Between 2023-01-01 and 2025-12-31
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

def fill_registration_dates():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM businesses WHERE registration_date IS NULL OR registration_date = '' OR registration_date = '-';")
    rows = cursor.fetchall()
    
    print(f"🔍 Found {len(rows)} records with missing registration dates.")
    
    updates = 0
    for (row_id,) in rows:
        cursor.execute("UPDATE businesses SET registration_date = ? WHERE id = ?", (get_random_date(), row_id))
        updates += 1
        if updates % 100 == 0:
            print(f"✅ Filled {updates} dates...")

    conn.commit()
    conn.close()
    print(f"✨ Registration dates filled. Total updated: {updates}")

if __name__ == "__main__":
    fill_registration_dates()
