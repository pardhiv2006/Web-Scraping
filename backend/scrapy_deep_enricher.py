import scrapy
from scrapy.crawler import CrawlerProcess
import re
import os
import sys
import logging
import urllib.parse
from sqlalchemy.orm import Session

# Add the project root to sys.path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.smart_scraper import _is_valid_name

CEO_RE = re.compile(
    r'(?:CEO|Chief Executive|Founder|Co-Founder|President|Owner|Managing Director|Managing Partner|Principal)\s*[:|\-]?\s*'
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    re.IGNORECASE
)

class DeepEnrichSpider(scrapy.Spider):
    name = "deep_enrich_spider"
    
    def __init__(self, limit=0, *args, **kwargs):
        super(DeepEnrichSpider, self).__init__(*args, **kwargs)
        self.db = SessionLocal()
        query = self.db.query(Business).filter(
            (Business.linkedin_url == None) | (Business.linkedin_url == "") |
            (Business.ceo_name == None) | (Business.ceo_name == "")
        )
        self.businesses = query.order_by(Business.id).limit(limit or 10000).all()
        self.logger.info(f"DeepEnrichSpider initialized with {len(self.businesses)} businesses missing LinkedIn or CEO data.")

    def start_requests(self):
        for biz in self.businesses:
            meta = {"biz_id": biz.id, "biz_name": biz.company_name, "state": biz.state}
            
            if not biz.linkedin_url:
                query = f'"{biz.company_name}" {biz.state or ""} LinkedIn company'
                url = f"https://duckduckgo.com/lite/?q={urllib.parse.quote_plus(query)}"
                yield scrapy.Request(
                    url, 
                    callback=self.parse_linkedin, 
                    meta=meta,
                    dont_filter=True,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                )
            
            if not biz.ceo_name:
                query = f'"{biz.company_name}" {biz.state or ""} CEO OR Founder'
                url = f"https://duckduckgo.com/lite/?q={urllib.parse.quote_plus(query)}"
                yield scrapy.Request(
                    url, 
                    callback=self.parse_ceo, 
                    meta=meta,
                    dont_filter=True,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                )

    def parse_linkedin(self, response):
        biz_id = response.meta["biz_id"]
        biz_name = response.meta["biz_name"]
        
        links = response.css("a.result-link::attr(href)").getall()
        for href in links:
            if "//duckduckgo.com/l/?" in href:
                try:
                    href = urllib.parse.unquote(href.split("?uddg=")[1].split("&")[0])
                except Exception: continue
            
            if "linkedin.com/company/" in href or "linkedin.com/in/" in href:
                href = href.split("?")[0] # remove tracking params
                yield {
                    "id": biz_id,
                    "company_name": biz_name,
                    "linkedin_url": str(href)
                }
                break

    def parse_ceo(self, response):
        biz_id = response.meta["biz_id"]
        biz_name = response.meta["biz_name"]
        
        snippets = response.css("tr").getall() 
        full_text = " ".join(response.css("td.result-snippet::text").getall() + response.css("a.result-link::text").getall())
        
        # 1. Regex search on full text
        m = CEO_RE.search(full_text)
        if m:
            candidate = m.group(1).strip()
            if _is_valid_name(candidate):
                yield {
                    "id": biz_id,
                    "company_name": biz_name,
                    "ceo_name": str(candidate)
                }
                return
                
        # 2. Title format search
        titles = response.css("a.result-link::text").getall()
        for t in titles:
            name_match = re.match(r'^([^\|\-]+?)\s+(?:-|\|)\s+(?:CEO|Founder|Owner|President)', t, re.IGNORECASE)
            if name_match:
                candidate = name_match.group(1).strip()
                if _is_valid_name(candidate):
                    yield {
                        "id": biz_id,
                        "company_name": biz_name,
                        "ceo_name": str(candidate)
                    }
                    return

    def closed(self, reason):
        self.db.close()

class DeepEnrichPipeline:
    def process_item(self, item, spider):
        db = SessionLocal()
        try:
            biz = db.query(Business).filter(Business.id == item["id"]).first()
            if biz:
                updated = False
                if item.get("linkedin_url") and not biz.linkedin_url:
                    biz.linkedin_url = item["linkedin_url"]
                    updated = True
                
                if item.get("ceo_name") and not biz.ceo_name:
                    biz.ceo_name = item["ceo_name"]
                    updated = True
                
                if updated:
                    db.commit()
                    spider.logger.info(f"Updated record {item['id']}: {item['company_name']} -> {item}")
        finally:
            db.close()
        return item

def run_deep_enricher(limit=0):
    settings = {
        'ITEM_PIPELINES': {__name__ + '.DeepEnrichPipeline': 300},
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 1.5,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'COOKIES_ENABLED': False,
        'LOG_LEVEL': 'INFO'
    }
    
    process = CrawlerProcess(settings)
    process.crawl(DeepEnrichSpider, limit=limit)
    process.start()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    run_deep_enricher(limit=args.limit)
