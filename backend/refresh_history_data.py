import sqlite3
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("HistoryRefresher")

def refresh_history():
    conn = sqlite3.connect('backend/businesses.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 1. Fetch all history items
    cursor.execute("SELECT id, country, states FROM search_history")
    history_items = cursor.fetchall()
    
    logger.info(f"Refreshing {len(history_items)} history snapshots...")
    
    updated_count = 0
    
    for item in history_items:
        hid = item['id']
        country = item['country']
        try:
            states = json.loads(item['states'])
        except:
            continue
            
        # 2. Fetch current authoritative data from businesses table for this search criteria
        # We need to handle the same state mapping logic as the app
        from services.scrape_service import STATE_MAPPING
        norm_states = [STATE_MAPPING.get(s.upper(), s.upper()).upper() for s in states]
        
        placeholders = ', '.join(['?'] * len(norm_states))
        query = f"""
            SELECT * FROM businesses 
            WHERE UPPER(country) = ? AND UPPER(state) IN ({placeholders})
            ORDER BY scraped_at DESC
        """
        params = [country.upper()] + norm_states
        
        cursor.execute(query, params)
        db_rows = cursor.fetchall()
        
        if not db_rows:
            logger.warning(f"History ID {hid}: No records found in businesses table for {country}/{states}")
            continue
            
        # Convert rows to dicts
        records = []
        for row in db_rows:
            # We want to use the to_dict logic if possible, but we're in a script.
            # I'll manually convert based on known columns or just use the row dict.
            d = dict(row)
            records.append(d)
            
        total = len(records)
        pagination_meta = {
            "total": total,
            "pages": max(1, -(-total // 50)),
            "limit": 50,
        }
        
        # 3. Update history item
        cursor.execute(
            "UPDATE search_history SET result_count = ?, result_data = ?, pagination_meta = ? WHERE id = ?",
            (total, json.dumps(records), json.dumps(pagination_meta), hid)
        )
        updated_count += 1
        
    conn.commit()
    conn.close()
    logger.info(f"Successfully refreshed {updated_count} history items.")

if __name__ == "__main__":
    # We need to add the backend dir to path to import services.scrape_service
    import os
    import sys
    sys.path.append(os.path.join(os.getcwd(), 'backend'))
    refresh_history()
