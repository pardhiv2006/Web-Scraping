
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import json
import re
import os
import sys
from urllib.parse import urljoin, quote_plus
from sqlalchemy.orm import Session
from bs4 import BeautifulSoup

# Add the project root to sys.path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.smart_scraper import smart_extract, JUNK_DOMAINS, guess_company_website

class BusinessSpider(scrapy.Spider):
    name = "business_spider"
    
    def __init__(self, limit=0, skip_done=True, *args, **kwargs):
        super(BusinessSpider, self).__init__(*args, **kwargs)
        self.db = SessionLocal()
        query = self.db.query(Business)
        if skip_done:
            # Skip if we already have website AND email
            query = query.filter(
                (Business.website == None) | (Business.website == "") |
                (Business.email == None) | (Business.email == "")
            )
        
        self.businesses = query.order_by(Business.id).limit(limit or 10000).all()
        self.logger.info(f"Spider initialized with {len(self.businesses)} businesses")

    def start_requests(self):
        for biz in self.businesses:
            # 1. Try Domain Guessing First (Ultra Fast, No API required)
            guessed_url = guess_company_website(biz.company_name, biz.country or "")
            if guessed_url:
                self.logger.info(f"Guessed website for {biz.company_name}: {guessed_url}")
                yield scrapy.Request(
                    guessed_url,
                    callback=self.parse_website,
                    meta={"biz_id": biz.id, "biz_name": biz.company_name, "website": guessed_url},
                    dont_filter=True
                )
                continue

            # 2. Fallback to Search Engine (Programmatic Discovery)
            query = f'"{biz.company_name}" {biz.state or ""} {biz.country or ""} official website'
            url = f"https://duckduckgo.com/lite/?q={quote_plus(query)}"
            yield scrapy.Request(
                url, 
                callback=self.parse_discovery, 
                meta={"biz_id": biz.id, "biz_name": biz.company_name},
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            )

    def parse_discovery(self, response):
        biz_id = response.meta["biz_id"]
        biz_name = response.meta["biz_name"]
        
        # DDG Lite results
        links = response.css("a.result-link::attr(href)").getall()
        website = None
        for href in links:
            if "//duckduckgo.com/l/?" in href:
                import urllib.parse
                try:
                    href = urllib.parse.unquote(href.split("?uddg=")[1].split("&")[0])
                except: continue
            
            if href.startswith("http") and not any(j in href.lower() for j in JUNK_DOMAINS):
                website = href
                break
        
        if website:
            self.logger.info(f"Found website for {biz_name}: {website}")
            yield scrapy.Request(
                website,
                callback=self.parse_website,
                meta={"biz_id": biz_id, "biz_name": biz_name, "website": website}
            )
        else:
            self.logger.warning(f"No website found for {biz_name}")

    def parse_website(self, response):
        biz_id = response.meta["biz_id"]
        biz_name = response.meta["biz_name"]
        website = response.meta["website"]
        
        # Use our existing smart_extract logic (AnyPicker-style)
        data = smart_extract(website, company_name=biz_name, html=response.text)
        
        yield {
            "id": biz_id,
            "company_name": biz_name,
            "data": data
        }

    def closed(self, reason):
        self.db.close()

class BusinessPipeline:
    def process_item(self, item, spider):
        db = SessionLocal()
        try:
            biz = db.query(Business).filter(Business.id == item["id"]).first()
            if biz:
                data = item["data"]
                updated = False
                for field in ["email", "phone", "website", "ceo_name", "ceo_email", "linkedin_url", "description", "industry"]:
                    val = data.get(field)
                    if val and not getattr(biz, field):
                        setattr(biz, field, val)
                        updated = True
                
                if updated:
                    db.commit()
                    spider.logger.info(f"Updated record {item['id']}: {item['company_name']}")
        finally:
            db.close()
        return item

def run_enricher(limit=50, skip_done=True):
    settings = {
        'ITEM_PIPELINES': {__name__ + '.BusinessPipeline': 300},
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 0.5,
        'COOKIES_ENABLED': False,
        'LOG_LEVEL': 'INFO',
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }
    
    process = CrawlerProcess(settings)
    process.crawl(BusinessSpider, limit=limit, skip_done=skip_done)
    process.start()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--skip-done", dest="skip_done", action="store_true")
    parser.add_argument("--no-skip", dest="skip_done", action="store_false")
    parser.set_defaults(skip_done=True)
    args = parser.parse_args()
    run_enricher(limit=args.limit, skip_done=args.skip_done)
