import requests

API_BASE = "http://localhost:8000/api"

def test():
    # 1. Check countries
    res = requests.get(f"{API_BASE}/countries")
    countries = res.json().get('countries', [])
    country_names = [c['name'] for c in countries]
    print(f"Countries: {country_names}")
    
    if "India" in country_names or "South Africa" in country_names:
        print("❌ India or South Africa still in dropdown!")
    else:
        print("✅ India and South Africa removed from dropdown.")
        
    # 2. Check states for UK
    res = requests.get(f"{API_BASE}/countries/UK/states")
    states = res.json().get('states', [])
    state_names = [s['name'] for s in states]
    print(f"UK States: {state_names}")
    
    if "SCT" in state_names:
        print("❌ SCT still in dropdown!")
    if "Scotland" in state_names:
        print("✅ Scotland found in dropdown.")
    
    # 3. Check business count for Scotland
    res = requests.get(f"{API_BASE}/businesses?country=UK&state=Scotland&limit=100")
    total = res.json().get('total', 0)
    print(f"Scotland Business Count (API): {total}")
    
    if total >= 68:
        print(f"✅ Scotland count is {total} (>= 68).")
    else:
        print(f"❌ Scotland count is {total} (< 68)!")

if __name__ == "__main__":
    test()
