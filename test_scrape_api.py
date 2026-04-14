import requests

res = requests.post("http://localhost:8000/api/scrape", json={"country": "UK", "states": ["SCT"]}, headers={"Authorization": "Bearer TEST"})
print(res.status_code)
print(res.text[:500])
