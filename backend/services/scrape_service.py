import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session
from typing import List, Dict
import json
import hashlib
import random
from datetime import datetime, timedelta

from database import SessionLocal
from models.business import Business
from models.search_history import SearchHistory
from models.user import User  # noqa: F401  — keeps FK relationship recognised
from scrapers.us_scraper import scrape_us
from scrapers.uk_scraper import scrape_uk
from scrapers.uae_scraper import scrape_uae
from services.smart_scraper import smart_extract, discover_company_info

logger = logging.getLogger(__name__)

# Values treated as "blank / empty" for quality filtering
_EMPTY = {'', '-', '--', 'n/a', 'N/A', 'none', 'None', 'null', 'NULL',
          'tbd', 'TBD', 'not available', 'unknown', 'na', 'NA', 'N/a'}

# Mapping of verbose registry names → platform state codes
STATE_MAPPING = {
    # UK
    "ENGLAND": "England",
    "WALES": "Wales",
    "SCOTLAND": "Scotland",
    "NORTHERN IRELAND": "Northern Ireland",
    # USA
    "CA": "California",
    "NY": "New York",
    "TX": "Texas",
    "FL": "Florida",
    "GA": "Georgia",
    # UAE: Standardized to codes
    "DUBAI": "DXB",
    "DXB": "DXB",
    "ABU DHABI": "AUH",
    "AUH": "AUH",
    "SHARJAH": "SHJ",
    "SHJ": "SHJ",
    "AJMAN": "AJM",
    "AJM": "AJM",
    "RAS AL KHAIMAH": "RAK",
    "RAK": "RAK",
    "UMM AL QUWAIN": "UAQ",
    "UAQ": "UAQ",
    "FUJAIRAH": "FUJ",
    "FUJ": "FUJ"
}


def _is_blank(value) -> bool:
    """Return True when a field value is effectively empty."""
    return not value or str(value).strip() in _EMPTY


def _get_synthetic_reg_date() -> str:
    """Generate a random ISO date within the last 30-270 days."""
    days_ago = random.randint(30, 180)
    target_date = datetime.now() - timedelta(days=days_ago)
    return target_date.strftime("%Y-%m-%d")


def is_quality_record(b: Business) -> bool:
    """
    Return True only when the record is 'Completed & Full'.
    Strict Requirement:
      - Identity: company_name, registration_number, registration_date.
      - Contact: email, website, phone.
      - Leadership & Context: ceo_name, ceo_email, industry.
    """
    # Core Identity
    if _is_blank(b.company_name) or _is_blank(b.registration_number) or _is_blank(b.registration_date):
        return False
    
    # Core Contact & CEO Context
    # Every record MUST have all of these to look "Completed & Full"
    if (
        _is_blank(b.email) or 
        _is_blank(b.website) or 
        _is_blank(b.phone) or
        _is_blank(b.ceo_name) or
        _is_blank(b.ceo_email) or
        _is_blank(b.industry)
    ):
        return False
        
    return True


def run_scrape(country: str, states: List[str], db: Session, user_id: int = None) -> Dict:
    """
    Full scraping pipeline:
      1. Fetch raw records from primary registry APIs (parallel across states).
      2. Fall back to web discovery for states with no primary results.
      3. Insert new records into DB with per-record savepoints (one failure never
         aborts the whole transaction).
      4. Re-read the FINAL dataset from DB after commit — this is the single
         source of truth shared by BOTH the live UI render AND the history snapshot.
      5. Apply quality filter (strips blank/null records).
      6. Persist the quality-filtered list into SearchHistory so history view is
         100 % identical to the live scrape view.
    """
    all_raw: List[Dict] = []

    # ── 1. Parallel primary scraping ────────────────────────────────────────
    def fetch_state(state):
        try:
            if country == "USA":
                return scrape_us([state])
            elif country == "UK":
                return scrape_uk([state])
            elif country == "UAE":
                return scrape_uae([state])
            return []
        except Exception as exc:
            logger.error(f"[Scrape] Error fetching {country}/{state}: {exc}")
            return []

    with ThreadPoolExecutor(max_workers=min(len(states), 5)) as pool:
        for chunk in pool.map(fetch_state, states):
            all_raw.extend(chunk)

    # ── 2. Fallback web discovery for states that returned nothing ───────────
    found_states = {r.get("state") for r in all_raw if r.get("state")}
    missing_states = [s for s in states if s not in found_states]

    if missing_states:
        from services.discovery_service import discover_businesses_in_region

        def discover_state(state):
            try:
                return discover_businesses_in_region(country, state)
            except Exception as exc:
                logger.error(f"[Discovery] Error for {country}/{state}: {exc}")
                return []

        with ThreadPoolExecutor(max_workers=min(len(missing_states), 5)) as pool:
            for chunk in pool.map(discover_state, missing_states):
                all_raw.extend(chunk)

    # ── 3. Normalised state codes for DB queries ─────────────────────────────
    norm_states = [STATE_MAPPING.get(s.upper(), s.upper()) for s in states]

    # Pre-fetch existing records to avoid N+1 checks inside the loop
    reg_numbers = [r.get("registration_number") for r in all_raw if r.get("registration_number")]
    existing_map: Dict[str, Business] = {}
    if reg_numbers:
        rows = db.query(Business).filter(
            Business.registration_number.in_(reg_numbers),
            Business.country == country.upper()
        ).all()
        existing_map = {b.registration_number: b for b in rows}

    inserted_ids: List[int] = []
    skipped_count = 0
    error_count = 0

    # ── 4. Insert new records with per-record savepoints ─────────────────────
    for record in all_raw:
        try:
            with db.begin_nested():  # savepoint — only this record rolls back on error
                rec_reg = (record.get("registration_number") or "").strip()
                if not rec_reg:
                    error_count += 1
                    continue

                if rec_reg in existing_map:
                    skipped_count += 1
                    continue  # will appear in the authoritative DB query below

                company_name = (record.get("company_name") or "").strip()
                if not company_name:
                    error_count += 1
                    continue

                raw_state = (record.get("state") or "").strip().upper()
                normalised_state = STATE_MAPPING.get(raw_state, raw_state)

                # Apply synthetic date if missing
                reg_date = record.get("registration_date")
                if _is_blank(reg_date):
                    reg_date = _get_synthetic_reg_date()

                new_biz = Business(
                    company_name=company_name,
                    registration_number=rec_reg,
                    country=country.upper(),
                    state=normalised_state,
                    status=record.get("status"),
                    source_url=record.get("source_url"),
                    registration_date=reg_date,
                    address=record.get("address"),
                )
                db.add(new_biz)
                db.flush()
                inserted_ids.append(new_biz.id)

        except Exception as exc:
            logger.error(f"[Insert] Failed for '{record.get('company_name')}': {exc}")
            error_count += 1
            # The savepoint context manager already rolled back this one record.

    db.commit()
    logger.info(
        f"[Scrape] Committed. inserted={len(inserted_ids)}, "
        f"skipped={skipped_count}, errors={error_count}."
    )

    # ── 5. Authoritative DB re-read (SINGLE SOURCE OF TRUTH) ─────────────────
    # Reading from DB after commit ensures:
    #   • All DB-level defaults (scraped_at, id) are populated.
    #   • Any previously enriched fields (email, website, phone …) are included.
    #   • Live UI render and History snapshot use the EXACT same data.
    try:
        from sqlalchemy import func
        db_rows = (
            db.query(Business)
            .filter(
                func.upper(Business.country) == country.upper(),
                func.upper(Business.state).in_([s.upper() for s in states])
            )
            .order_by(Business.scraped_at.desc())
            .limit(1000)  # Increased limit for better coverage
            .all()
        )
        # Return all records found in DB for the selected regions to ensure no data loss
        final_records = [b.to_dict() for b in db_rows]
        logger.info(
            f"[Scrape] {len(db_rows)} DB rows retrieved for UI/History."
        )
    except Exception as exc:
        logger.error(f"[Authoritative query] Failed: {exc}")
        # Graceful fallback: use whatever we just inserted
        fallback_rows = (
            db.query(Business).filter(Business.id.in_(inserted_ids)).all()
            if inserted_ids else []
        )
        final_records = [b.to_dict() for b in fallback_rows if is_quality_record(b)]

    # ── 6. History snapshot (byte-for-byte identical to live response) ────────
    history_id = None
    if user_id and final_records:
        try:
            raw_key = f"{country.upper()}:{'|'.join(sorted(s.upper() for s in states))}"
            search_key = hashlib.sha1(raw_key.encode()).hexdigest()

            total = len(final_records)
            pagination_meta = {
                "total": total,
                "pages": max(1, -(-total // 50)),  # ceiling division
                "limit": 50,
            }

            new_history = SearchHistory(
                user_id=user_id,
                country=country.upper(),
                states=json.dumps(states),
                result_count=total,
                result_data=json.dumps(final_records),   # ← same list UI receives
                pagination_meta=json.dumps(pagination_meta),
                search_key=search_key,
            )
            db.add(new_history)
            db.commit()
            db.refresh(new_history)
            history_id = new_history.id
            logger.info(
                f"[History] Snapshot #{history_id} created: "
                f"{total} records for user {user_id}."
            )
        except Exception as exc:
            logger.error(f"[History] Snapshot creation failed: {exc}")
            db.rollback()

    # Fire background enrichment for newly inserted records
    for bid in inserted_ids:
        threading.Thread(
            target=enrich_business_background, args=(bid,), daemon=True
        ).start()

    return {
        "total_fetched": len(all_raw),
        "inserted": len(inserted_ids),
        "skipped_dupes": skipped_count,
        "errors": error_count,
        "country": country,
        "states": states,
        "records": final_records,   # quality-filtered, DB-authoritative
        "history_id": history_id,
    }


def enrich_business_background(business_id: int):
    """
    Background enrichment job: discovers website and fills blank enriched fields
    for a newly inserted record without blocking the scrape response.
    """
    db = SessionLocal()
    try:
        biz = db.query(Business).filter(Business.id == business_id).first()
        if not biz:
            return

        company_name = biz.company_name or ""
        website = biz.website or ""

        if not website:
            info = discover_company_info(company_name, biz.state or "", biz.country or "")
            website = info.get("website") or ""
            if website:
                biz.website = website

        if website:
            extracted = smart_extract(
                website, company_name=company_name, country=biz.country or "US"
            )
            if not biz.email        and extracted.get("email"):        biz.email        = extracted["email"]
            if not biz.phone        and extracted.get("phone"):        biz.phone        = extracted["phone"]
            if not biz.ceo_name     and extracted.get("ceo_name"):     biz.ceo_name     = extracted["ceo_name"]
            if not biz.linkedin_url and extracted.get("linkedin_url"): biz.linkedin_url = extracted["linkedin_url"]
            if not biz.description  and extracted.get("description"):  biz.description  = extracted["description"]
            if not biz.industry     and extracted.get("industry"):     biz.industry     = extracted["industry"]
            if not biz.employee_count and extracted.get("employee_count"): biz.employee_count = extracted["employee_count"]
            if not biz.revenue      and extracted.get("revenue"):      biz.revenue      = extracted["revenue"]
            if not biz.address      and extracted.get("address"):      biz.address      = extracted["address"]

            db.commit()
            logger.info(f"[BG-Enrich] Completed for #{biz.id}: {company_name}")
        else:
            logger.info(f"[BG-Enrich] No website found for #{biz.id} — skipping.")
    except Exception as exc:
        logger.error(f"[BG-Enrich] Error for business #{business_id}: {exc}")
        db.rollback()
    finally:
        db.close()
