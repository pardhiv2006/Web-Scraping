"""
Bulk Enrichment Script — fills all blank columns for every company.
Uses free-only tools (DuckDuckGo + website scraping, no API keys needed).

Usage:
    cd backend
    ./venv/bin/python3 bulk_enrich.py                  # all records
    ./venv/bin/python3 bulk_enrich.py --limit 20       # first 20
    ./venv/bin/python3 bulk_enrich.py --skip-done      # skip already enriched
    ./venv/bin/python3 bulk_enrich.py --batch 5        # 5 at a time
"""
import sys
import os
import logging
import argparse
import time
from concurrent.futures import ThreadPoolExecutor

# Ensure backend directory is on the path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(backend_dir, 'businesses.db')}"

from database import SessionLocal
from models.business import Business
from services.enrichment_service import enrich_business

# Logging — both console and file
log_path = os.path.join(backend_dir, "enrichment.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_path, mode="a", encoding="utf-8"),
    ]
)
logger = logging.getLogger("BulkEnrich")


def parse_args():
    p = argparse.ArgumentParser(description="Bulk enrichment runner")
    p.add_argument("--limit",     type=int, default=0,    help="Max records to process (0=all)")
    p.add_argument("--batch",     type=int, default=10,   help="Records per batch before pause")
    p.add_argument("--pause",     type=float, default=2.0, help="Seconds to pause between batches")
    p.add_argument("--skip-done", action="store_true",    help="Skip records that already have a website")
    p.add_argument("--workers",   type=int, default=5,    help="Number of parallel workers")
    return p.parse_args()


def run_bulk_enrich():
    args = parse_args()
    db = SessionLocal()

    try:
        query = db.query(Business)
        if args.skip_done:
            # Skip only if ALL major fields are present
            query = query.filter(
                (Business.website == None) | (Business.website == "") |
                (Business.email == None) | (Business.email == "") |
                (Business.ceo_name == None) | (Business.ceo_name == "") |
                (Business.linkedin_url == None) | (Business.linkedin_url == "")
            )

        total_q = query.count()
        businesses = query.order_by(Business.id).limit(args.limit or total_q).all()
        total = len(businesses)

        logger.info("=" * 60)
        logger.info(f"BULK ENRICHMENT STARTING — {total} records to process")
        logger.info(f"Batch size: {args.batch}  |  Pause: {args.pause}s  |  Skip done: {args.skip_done}")
        logger.info(f"Log file: {log_path}")
        logger.info("=" * 60)

        enriched_count = 0
        skipped_count  = 0
        error_count    = 0

        # Use ThreadPoolExecutor for parallel processing
        def process_one(biz_id, biz_name, location):
            try:
                # We need a new session per thread to avoid race conditions
                thread_db = SessionLocal()
                try:
                    updated = enrich_business(thread_db, biz_id)
                    return updated, None
                finally:
                    thread_db.close()
            except Exception as e:
                return False, e

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(process_one, biz.id, biz.company_name, f"{biz.country}/{biz.state}"): biz 
                for biz in businesses
            }
            
            for i, future in enumerate(futures, 1):
                biz = futures[future]
                updated, error = future.result()
                
                pct = (i / total) * 100
                status = "✓ Updated" if updated else "○ No Change"
                if error:
                    status = f"✗ Error: {error}"
                    error_count += 1
                elif updated:
                    enriched_count += 1
                else:
                    skipped_count += 1
                
                logger.info(f"[{i}/{total}] ({pct:.0f}%) | {biz.company_name:30} | {status}")

                # Batch pause logic (simplified for parallel)
                if i % args.batch == 0:
                    logger.info(f"  [Batch] Progress: {enriched_count} enriched, {skipped_count} skipped, {error_count} errors")
                    if args.pause > 0:
                        time.sleep(args.pause)

        logger.info("=" * 60)
        logger.info("BULK ENRICHMENT COMPLETE")
        logger.info(f"  Total processed : {total}")
        logger.info(f"  Newly enriched  : {enriched_count}")
        logger.info(f"  Skipped         : {skipped_count}")
        logger.info(f"  Errors          : {error_count}")
        logger.info("=" * 60)

        # Print final DB stats
        from sqlalchemy import func
        total_db     = db.query(Business).count()
        has_website  = db.query(Business).filter(Business.website != None, Business.website != "").count()  # noqa
        has_email    = db.query(Business).filter(Business.email   != None, Business.email   != "").count()  # noqa
        has_phone    = db.query(Business).filter(Business.phone   != None, Business.phone   != "").count()  # noqa
        has_ceo      = db.query(Business).filter(Business.ceo_name != None).count()  # noqa
        has_linkedin = db.query(Business).filter(Business.linkedin_url != None).count()  # noqa

        logger.info("\nDATABASE COVERAGE AFTER ENRICHMENT:")
        logger.info(f"  Website  : {has_website}/{total_db}  ({has_website/total_db*100:.1f}%)")
        logger.info(f"  Email    : {has_email}/{total_db}    ({has_email/total_db*100:.1f}%)")
        logger.info(f"  Phone    : {has_phone}/{total_db}    ({has_phone/total_db*100:.1f}%)")
        logger.info(f"  CEO name : {has_ceo}/{total_db}      ({has_ceo/total_db*100:.1f}%)")
        logger.info(f"  LinkedIn : {has_linkedin}/{total_db} ({has_linkedin/total_db*100:.1f}%)")

    finally:
        db.close()


if __name__ == "__main__":
    run_bulk_enrich()
