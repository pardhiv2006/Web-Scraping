import sqlite3
import os

DB_PATH = "businesses.db"

def run_db_normalization():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("🛠️ Normalizing UAE Country and States...")
    
    # 1. Standardize Country to 'UAE'
    cursor.execute("""
        UPDATE businesses 
        SET country = 'UAE' 
        WHERE UPPER(country) IN ('UNITED ARAB EMIRATES', 'AE', 'ARE', 'UAE');
    """)
    print(f"   ✅ Standardized {cursor.rowcount} UAE country labels.")

    # 2. Standardize UAE Emirates to 3-letter codes
    uae_map = {
        "DUBAI": "DXB", "DXB": "DXB",
        "ABU DHABI": "AUH", "AUH": "AUH",
        "SHARJAH": "SHJ", "SHJ": "SHJ",
        "AJMAN": "AJM", "AJM": "AJM",
        "RAS AL KHAIMAH": "RAK", "RAK": "RAK",
        "UMM AL QUWAIN": "UAQ", "UAQ": "UAQ",
        "FUJAIRAH": "FUJ", "FUJ": "FUJ"
    }
    
    for full, code in uae_map.items():
        cursor.execute("UPDATE businesses SET state = ? WHERE country = 'UAE' AND UPPER(state) = ?;", (code, full))
        if cursor.rowcount > 0:
            print(f"   ✅ Updated {cursor.rowcount} records for {full} -> {code}")

    # 3. Deduplicate (Company Name + Email)
    # We'll identify duplicates and keep the one with the highest ID (most recent)
    print("🧹 Deduplicating by Company Name + Email...")
    cursor.execute("""
        DELETE FROM businesses 
        WHERE id NOT IN (
            SELECT MAX(id) 
            FROM businesses 
            GROUP BY LOWER(TRIM(company_name)), LOWER(TRIM(COALESCE(email, '')))
        );
    """)
    print(f"   ✅ Removed {cursor.rowcount} duplicate records.")

    conn.commit()
    conn.close()
    print("✨ Database normalization complete.")

if __name__ == "__main__":
    run_db_normalization()
