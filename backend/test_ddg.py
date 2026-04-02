import requests
import urllib.parse
from bs4 import BeautifulSoup

def search(query):
    encoded = urllib.parse.quote_plus(query)
    url = f"https://html.duckduckgo.com/html/?q={encoded}"
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}
    resp = requests.get(url, headers=h)
    print("Status:", resp.status_code)
    soup = BeautifulSoup(resp.text, "html.parser")
    for result in soup.select(".result__body"):
        title = result.select_one(".result__title").text.strip() if result.select_one(".result__title") else ""
        snippet = result.select_one(".result__snippet").text.strip() if result.select_one(".result__snippet") else ""
        link = result.select_one(".result__url").get("href") if result.select_one(".result__url") else ""
        print(title, snippet, link)

search("test")
