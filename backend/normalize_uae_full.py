import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("UAE_Full_Name_Normalizer")

# Mapping to ensure all UAE states use Full Names
UAE_MAPPING = {
    "DXB": "Dubai",
    "DUBAI": "Dubai",
    "AUH": "Abu Dhabi",
    "ABU DHABI": "Abu Dhabi",
    "SHJ": "Sharjah",
    "SHARJAH": "Sharjah",
    "AJM": "Ajman",
    "AJMAN": "Ajman",
    "RAK": "Ras Al Khaimah",
    "RAS AL KHAIMAH": "Ras Al Khaimah",
    "UAQ": "Umm Al Quwain",
    "UMM AL QUWAIN": "Umm Al Quwain",
    "FUJ": "Fujairah",
    "FUJAIRAH": "Fujairah"
}

def normalize_businesses():
    conn = sqlite3.connect('backend/businesses.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, state FROM businesses WHERE country = 'UAE'")
    rows = cursor.fetchall()
    
    logger.info(f"Normalizing UAE states in 'businesses' table ({len(rows)} records)...")
    
    updated = 0
    for rid, state in rows:
        if not state: continue
        norm = UAE_MAPPING.get(state.upper())
        if norm and norm != state:
            cursor.execute("UPDATE businesses SET state = ? WHERE id = ?", (norm, rid))
            updated += 1
            
    conn.commit()
    conn.close()
    logger.info(f"Successfully updated {updated} records to full state names.")

def normalize_history():
    import json
    conn = sqlite3.connect('backend/businesses.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, states FROM search_history WHERE country = 'UAE'")
    rows = cursor.fetchall()
    
    logger.info(f"Normalizing UAE states in 'search_history' table ({len(rows)} records)...")
    
    updated = 0
    for hid, states_json in rows:
        if not states_json: continue
        try:
            states = json.loads(states_json)
            new_states = [UAE_MAPPING.get(s.upper(), s) for s in states]
            if new_states != states:
                cursor.execute("UPDATE search_history SET states = ? WHERE id = ?", (json.dumps(new_states), hid))
                updated += 1
        except:
            continue
            
    conn.commit()
    conn.close()
    logger.info(f"Successfully updated {updated} history search keys.")

if __name__ == "__main__":
    normalize_businesses()
    normalize_history()
