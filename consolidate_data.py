
import sqlite3
import os

def consolidate_states():
    db_path = 'businesses.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # UK States
    uk_map = {
        "SCT": "Scotland", "SCOTLAND": "Scotland",
        "ENG": "England",  "ENGLAND":  "England",
        "WLS": "Wales",    "WALES":    "Wales",
        "NIR": "Northern Ireland", "NORTHERN IRELAND": "Northern Ireland",
    }
    
    # USA States (Standard mapping)
    us_map = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
        "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
        "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
        "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
        "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
        "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
        "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
        "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
        "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
        "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia"
    }

    cursor.execute("SELECT id, country, state FROM businesses")
    rows = cursor.fetchall()
    
    updates = 0
    for biz_id, country, state in rows:
        if not state: continue
        
        orig = state
        new_state = state.strip()
        upper = new_state.upper()
        
        if country == 'UK':
            new_state = uk_map.get(upper, new_state.title())
        elif country == 'USA':
            # Check if it's an abbreviation
            if upper in us_map:
                new_state = us_map[upper]
            else:
                # Check if it matches a full name (case insensitive)
                for abbr, name in us_map.items():
                    if upper == name.upper():
                        new_state = name
                        break
                else:
                    new_state = new_state.title()
        
        if new_state != orig:
            cursor.execute("UPDATE businesses SET state = ? WHERE id = ?", (new_state, biz_id))
            updates += 1

    conn.commit()
    print(f"✅ Consolidated {updates} state names.")
    
    # Also fix any remaining 'US' country codes (safety)
    cursor.execute("UPDATE businesses SET country = 'USA' WHERE country = 'US'")
    if cursor.rowcount > 0:
        print(f"✅ Fixed {cursor.rowcount} 'US' -> 'USA' country codes.")
        conn.commit()

    conn.close()

if __name__ == "__main__":
    consolidate_states()
