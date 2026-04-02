import requests
import urllib.parse
from bs4 import BeautifulSoup

def search_bing(query):
    encoded = urllib.parse.quote_plus(query)
    url = f"https://www.bing.com/search?q={encoded}"
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}
    resp = requests.get(url, headers=h)
    print("Status:", resp.status_code)
    soup = BeautifulSoup(resp.text, "html.parser")
    for li in soup.select("li.b_algo"):
        title = li.select_one("h2").text.strip() if li.select_one("h2") else ""
        snippet = li.select_one(".b_algoSlug, .b_caption p").text.strip() if li.select_one(".b_algoSlug, .b_caption p") else ""
        link = li.select_one("a").get("href") if li.select_one("a") else ""
        print("TITLE:", title)
        print("SNIPPET:", snippet)
        print("LINK:", link)
        print("---")

search_bing("Crown Technologies Inc CA CEO OR Founder")
