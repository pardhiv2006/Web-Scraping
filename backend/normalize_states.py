import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DatabaseNormalizer")

# Copy the mapping from scrape_service.py to be safe
STATE_MAPPING = {
    # UK
    "ENGLAND": "England",
    "WALES": "Wales",
    "SCOTLAND": "Scotland",
    "NORTHERN IRELAND": "Northern Ireland",
    # USA
    "CA": "California",
    "NY": "New York",
    "TX": "Texas",
    "FL": "Florida",
    "GA": "Georgia",
    # UAE emirates
    "DUBAI": "DXB",
    "DXB": "DXB",
    "ABU DHABI": "AUH",
    "AUH": "AUH",
    "SHARJAH": "SHJ",
    "SHJ": "SHJ",
    "AJMAN": "AJM",
    "AJM": "AJM",
    "RAS AL KHAIMAH": "RAK",
    "RAK": "RAK",
    "UMM AL QUWAIN": "UAQ",
    "UAQ": "UAQ",
    "FUJAIRAH": "FUJ",
    "FUJ": "FUJ"
}

def normalize_db():
    conn = sqlite3.connect('backend/businesses.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, state FROM businesses")
    rows = cursor.fetchall()
    
    logger.info(f"Checking {len(rows)} records for state normalization...")
    
    update_count = 0
    for rid, state in rows:
        if not state:
            continue
            
        upper_state = state.strip().upper()
        norm_state = STATE_MAPPING.get(upper_state)
        
        if norm_state and norm_state != state:
            cursor.execute("UPDATE businesses SET state = ? WHERE id = ?", (norm_state, rid))
            update_count += 1
            
    conn.commit()
    conn.close()
    logger.info(f"Successfully normalized {update_count} records.")

if __name__ == "__main__":
    normalize_db()
