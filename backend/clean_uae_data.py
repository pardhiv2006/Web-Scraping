import sqlite3
import re

def clean_uae_address(addr):
    if not addr: return addr
    
    # Common garbage phrases in search snippets
    garbage = [
        r'[\u0980-\u09FF]+', # Bengali characters seen in logs
        r'\d+,\d+\s+লিংকডইনে ফলোয়ার',
        r'Google has offices in nearly.*',
        r'Company profile page for.*',
        r'Find company information.*',
        r'Business Type:.*',
        r'Main Products:.*',
        r'Trade Capacity:.*',
        r'Production Capacity:.*',
        r'diligencia, providing.*',
        r'Feb \d+, \d+ .*',
        r'View a directory of our locations.*',
        r'[\w\.-]+@[\w\.-]+\.\w+', # Emails
        r'https?://\S+', # URLs
    ]
    
    cleaned = addr
    for pattern in garbage:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Try to keep just the location part
    # Look for common UAE cities
    cities = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Umm Al Quwain", "Ras Al Khaimah", "Fujairah", "Al Ain", "DXB", "AUH"]
    found_cities = [city for city in cities if city.lower() in cleaned.lower()]
    
    if found_cities:
        # If we have a mess, just use "City, UAE"
        # But try to see if there's a street-like number/name before the city
        parts = cleaned.split(",")
        useful_parts = []
        for p in parts:
            p = p.strip()
            if len(p) > 2 and not any(g in p.lower() for g in ["profile", "follow", "products", "capacity"]):
                useful_parts.append(p)
        
        if len(useful_parts) > 0:
            final_addr = ", ".join(useful_parts)
            # Cap it to avoid snippet overflow
            if len(final_addr) > 100:
                final_addr = f"{found_cities[0]}, United Arab Emirates"
            return final_addr
    
    return cleaned.strip(", ")

def run_cleanup():
    conn = sqlite3.connect('businesses.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, address, company_name FROM businesses WHERE country = 'UAE'")
    rows = cursor.fetchall()
    
    for biz_id, addr, name in rows:
        new_addr = clean_uae_address(addr)
        if new_addr != addr:
            cursor.execute("UPDATE businesses SET address = ? WHERE id = ?", (new_addr, biz_id))
            
    conn.commit()
    conn.close()
    print("UAE address cleanup complete.")

if __name__ == "__main__":
    run_cleanup()
