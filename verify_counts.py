import requests

API_BASE = "http://localhost:8000/api"

def test():
    # Check Wales count
    res = requests.get(f"{API_BASE}/businesses?country=UK&state=Wales&limit=100")
    total = res.json().get('total', 0)
    print(f"Wales Business Count (API): {total}")
    
    # Check Scotland count
    res = requests.get(f"{API_BASE}/businesses?country=UK&state=Scotland&limit=100")
    total_s = res.json().get('total', 0)
    print(f"Scotland Business Count (API): {total_s}")

if __name__ == "__main__":
    test()
