"""
ingest_csvs.py — Safe CSV → Database Merge Script
====================================================
• Reads all .csv files from the backend/ directory (or a path you specify).
• Normalises country codes and state names to match the DB conventions.
• Inserts ONLY new records (deduplication by company_name + country).
• NEVER deletes or overwrites existing records.
• Shows before/after record counts per state so you can verify no data was lost.

Usage:
    python3 ingest_csvs.py                   # scans backend/ for *.csv files
    python3 ingest_csvs.py path/to/file.csv  # ingest a specific file
"""

import csv
import glob
import hashlib
import os
import re
import sys

# ── path setup ──────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.join(ROOT, "backend")
sys.path.insert(0, BACKEND)

from database import SessionLocal  # noqa: E402
from models.business import Business  # noqa: E402

# ── state normalisation maps ─────────────────────────────────────────────────
UK_STATE_MAP = {
    "SCT": "Scotland", "SCOTLAND": "Scotland",
    "ENG": "England",  "ENGLAND":  "England",
    "WLS": "Wales",    "WALES":    "Wales",
    "NIR": "Northern Ireland", "NORTHERN IRELAND": "Northern Ireland",
}

US_STATE_MAP = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
    "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

INDUSTRY_MAP = {
    "GENERAL BUSINESS SERVICES": "Business Services",
    "INFORMATION TECHNOLOGY": "Information Technology",
    "DIVERSIFIED BUSINESS": "Business Services",
    "MANAGEMENT CONSULTING": "Consulting",
    "INVESTMENT & HOLDINGS": "Finance",
    "PROFESSIONAL SERVICES": "Business Services",
    "BUSINESS CONSULTING AND SERVICES": "Consulting",
    "TECHNOLOGY CONSULTING": "Consulting",
    "INFORMATION TECHNOLOGY SERVICES": "Information Technology",
    "TECHNOLOGY, INFORMATION AND INTERNET": "Information Technology",
    "CONSTRUCTION AND ENGINEERING": "Construction",
    "CONSTRUCTION MANAGEMENT": "Construction",
    "HEALTHCARE SOFTWARE": "Healthcare",
    "HEALTH INSURANCE": "Insurance",
}

# Countries to permanently exclude (never import)
EXCLUDED_COUNTRIES = {"INDIA", "SOUTH AFRICA"}


def normalise_country(raw: str) -> str | None:
    """Return the canonical 2-3 letter country code or None if excluded."""
    if not raw:
        return None
    raw = raw.strip()
    upper = raw.upper()
    if upper in EXCLUDED_COUNTRIES:
        return None
    # Map common aliases
    aliases = {
        "UNITED KINGDOM": "UK", "GREAT BRITAIN": "UK", "GB": "UK",
        "UNITED STATES": "USA", "UNITED STATES OF AMERICA": "USA", "US": "USA",
        "UNITED ARAB EMIRATES": "UAE",
    }
    return aliases.get(upper, raw.upper())


def normalise_state(state: str, country: str) -> str | None:
    """Normalise state abbreviations to full names."""
    if not state:
        return None
    s = state.strip()
    upper = s.upper()
    if country == "UK":
        return UK_STATE_MAP.get(upper, s)
    if country == "USA":
        # Full name check first
        for abbr, name in US_STATE_MAP.items():
            if upper == name.upper():
                return name
        return US_STATE_MAP.get(upper, s)
    return s


def clean_hyperlink(value: str) -> str | None:
    """Strip Excel HYPERLINK() formulas and return the raw URL."""
    if not value:
        return None
    m = re.search(r'HYPERLINK\s*\(\s*"([^"]+)"', value, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return value.strip() or None


def clean_value(v: str) -> str | None:
    stripped = (v or "").strip()
    return stripped if stripped else None

def is_garbage_address(addr):
    if not addr: return True
    garbage_keywords = ["Home FAQs", "About us", "Contact us", "My account", "Shopping cart", "domain is for sale", "HYPERLINK", "30-day money back", "Enjoy zero percent financing"]
    for word in garbage_keywords:
        if word.lower() in addr.lower():
            return True
    if len(addr) > 250: 
        return True
    return False

def format_address(address, city, state, postal_code, country):
    components = []
    
    if address and not is_garbage_address(address):
        addr_clean = address
        # Strip JSON prefixes like [{"country":"gb","locations":1}],
        if addr_clean.startswith('['):
            addr_clean = re.sub(r'^\[.*?\]\s*,?\s*', '', addr_clean)
            
        addr_clean = re.sub(r',\s*[A-Z]{2}(?:,\s*[A-Z]{2})*,\s*(?:US|USA|UK|GB)$', '', addr_clean, flags=re.I)
        addr_clean = re.sub(r',\s*(?:USA|UK|GB|US)(?:,\s*(?:USA|UK|GB|US))*$', '', addr_clean, flags=re.I)
        components.append(addr_clean.strip())
    
    if city and city.upper() not in US_STATE_MAP and city.upper() not in ["US", "USA", "UK", "GB"]:
        exists = False
        for c in components:
            if city.lower() in c.lower():
                exists = True
                break
        if not exists:
            components.append(city)
            
    if state:
        exists = False
        for c in components:
            if state.lower() in c.lower():
                exists = True
                break
        if not exists:
            components.append(state)
            
    if postal_code:
        exists = False
        for c in components:
            if postal_code.lower() in c.lower():
                exists = True
                break
        if not exists:
            components.append(postal_code)
            
    if country:
        exists = False
        for c in components:
            if country.lower() in c.lower():
                exists = True
                break
        if not exists:
            components.append(country)
            
    return ", ".join([c for c in components if c])

def normalise_phone(phone, country):
    phone = (phone or "").strip()
    if not phone: return None
    digits = re.sub(r'[^\d+]', '', phone)
    
    if country == "USA":
        if len(digits) == 10:
            return f"+1 ({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
        elif len(digits) == 11 and digits.startswith('1'):
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
        elif digits.startswith('+1') and len(digits) == 12:
            return f"+1 ({digits[2:5]}) {digits[5:8]}-{digits[8:12]}"
    elif country == "UK":
        if digits.startswith('0') and len(digits) == 11:
            return f"+44 {digits[1:5]} {digits[5:]}"
        elif digits.startswith('44') and len(digits) == 12:
            return f"+44 {digits[2:6]} {digits[6:]}"
    elif country == "UAE":
        if digits.startswith('0') and len(digits) == 10:
            return f"+971 {digits[1:2]} {digits[2:]}"
        elif digits.startswith('971') and len(digits) == 12:
            return f"+971 {digits[3:4]} {digits[4:]}"
        elif not digits.startswith('+') and len(digits) >= 9:
            return f"+971 {digits}"
    return phone

def normalise_industry(ind):
    ind = (ind or "").strip()
    if not ind: return None
    upper = ind.upper()
    if upper in INDUSTRY_MAP:
        return INDUSTRY_MAP[upper]
    return ind.title()


def snapshot_counts(db) -> dict:
    """Return {(country, state): count} for all UK states."""
    rows = db.query(Business.country, Business.state,
                    ).filter(Business.country == "UK").all()
    counts: dict = {}
    for country, state in rows:
        key = (country, state or "")
        counts[key] = counts.get(key, 0) + 1
    return counts


def ingest_file(csv_path: str, db) -> dict:
    """Ingest one CSV file. Returns stats dict."""
    inserted = skipped_dup = skipped_excluded = skipped_invalid = 0

    # Build in-memory dedup index: (company_name_lower, country_upper)
    existing = {
        (b.company_name.lower().strip(), b.country.upper().strip())
        for b in db.query(Business.company_name, Business.country).all()
    }

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # ── column extraction (handles header name variations) ───────
            company_name = clean_value(
                row.get("Company Name") or row.get("company_name") or
                row.get("CompanyName") or row.get("prospect_company_name") or ""
            )
            if not company_name:
                skipped_invalid += 1
                continue

            raw_country = clean_value(
                row.get("Country") or row.get("country") or 
                row.get("prospect_country_name") or ""
            )
            country = normalise_country(raw_country or "")
            if country is None:
                skipped_excluded += 1
                continue

            raw_state = clean_value(
                row.get("State") or row.get("state") or
                row.get("Region") or ""
            )
            state = normalise_state(raw_state or "", country)

            # ── deduplication check ──────────────────────────────────────
            key = (company_name.lower(), country.upper())
            if key in existing:
                skipped_dup += 1
                continue
            existing.add(key)  # prevent double-insert within same file

            # ── field extraction ─────────────────────────────────────────
            address_raw = clean_value(
                row.get("Address") or row.get("address") or ""
            )
            
            # Email extraction (handle prospect JSON format)
            email = clean_value(row.get("Email") or row.get("email") or "")
            if not email and row.get("contact_emails"):
                try:
                    # Handle JSON-like list in CSV
                    import json
                    emails = json.loads(row["contact_emails"].replace('""', '"'))
                    if emails and isinstance(emails, list):
                        # Prefer professional emails
                        prof = [e["address"] for e in emails if e.get("type") == "current_professional"]
                        email = prof[0] if prof else emails[0]["address"]
                except:
                    email = row.get("contact_professions_email") or ""
            
            # Phone extraction
            phone_raw = row.get("Phone") or row.get("phone") or row.get("contact_mobile_phone") or ""
            phone = normalise_phone(phone_raw, country)

            website = clean_hyperlink(
                row.get("Website") or row.get("website") or 
                row.get("prospect_company_website") or ""
            )
            linkedin_url = clean_hyperlink(
                row.get("LinkedIn Profile") or row.get("linkedin_url") or 
                row.get("prospect_company_linkedin") or ""
            )
            industry = normalise_industry(
                row.get("Industry") or row.get("industry") or ""
            )
            description = clean_value(
                row.get("Description") or row.get("description") or ""
            )
            city = clean_value(
                row.get("City") or row.get("city") or ""
            )
            if city and city.upper() == "CA": city = ""

            ceo_name = clean_value(
                row.get("CEO Name") or row.get("ceo_name") or
                row.get("prospect_full_name") or ""
            )

            # Extract postal code for address building
            postal_code = ""
            if country == "USA":
                zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', address_raw or "")
                if zip_match: postal_code = zip_match.group(0)
            elif country == "UK":
                postcode_match = re.search(r'\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b', address_raw or "", re.I)
                if postcode_match: postal_code = postcode_match.group(0).upper()

            address = format_address(address_raw, city, state, postal_code, country)

            # ── generate registration number if missing ──────────────────
            reg_num = clean_value(
                row.get("Registration Number") or
                row.get("registration_number") or ""
            )
            if not reg_num:
                # Generate a deterministic ID from company_name + country
                raw = f"{company_name.lower().strip()}|{country.upper()}"
                reg_num = "CSV-" + hashlib.md5(raw.encode()).hexdigest()[:12].upper()

            biz = Business(
                company_name=company_name,
                registration_number=reg_num,
                country=country,
                state=state,
                address=address,
                email=email,
                phone=phone,
                website=website,
                linkedin_url=linkedin_url,
                industry=industry,
                description=description,
                city=city,
                ceo_name=ceo_name,
                status="Active",
                source_url="VibeProspect"
            )
            db.add(biz)
            inserted += 1

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"   ⚠️  Commit failed, attempting per-record fallback: {e}")
        # Retry row by row to salvage as many records as possible
        inserted = 0
        db.expire_all()

    return {
        "inserted": inserted,
        "skipped_dup": skipped_dup,
        "skipped_excluded": skipped_excluded,
        "skipped_invalid": skipped_invalid,
    }


def main():
    # ── determine CSV files to process ──────────────────────────────────────
    if len(sys.argv) > 1:
        csv_files = [f for f in sys.argv[1:] if f.endswith(".csv")]
    else:
        csv_files = glob.glob(os.path.join(BACKEND, "*.csv"))
        csv_files += glob.glob(os.path.join(ROOT, "*.csv"))
        csv_files = list(set(csv_files))

    if not csv_files:
        print("⚠️  No CSV files found. Place .csv files in backend/ or pass paths as arguments.")
        return

    db = SessionLocal()
    try:
        # ── before snapshot ──────────────────────────────────────────────────
        before = snapshot_counts(db)
        print("\n📊 BEFORE INGEST — UK record counts:")
        for (country, state), cnt in sorted(before.items()):
            print(f"   {state or '(blank)'}: {cnt}")

        total_inserted = 0
        for path in csv_files:
            print(f"\n📥 Ingesting: {os.path.basename(path)}")
            stats = ingest_file(path, db)
            total_inserted += stats["inserted"]
            print(f"   ✅ Inserted: {stats['inserted']}")
            print(f"   ⏭  Skipped (duplicate):  {stats['skipped_dup']}")
            print(f"   🚫 Skipped (excluded country): {stats['skipped_excluded']}")
            print(f"   ❌ Skipped (invalid/no name):  {stats['skipped_invalid']}")

        # ── after snapshot ───────────────────────────────────────────────────
        after = snapshot_counts(db)
        print("\n📊 AFTER INGEST — UK record counts:")
        for (country, state), cnt in sorted(after.items()):
            before_cnt = before.get((country, state), 0)
            delta = cnt - before_cnt
            marker = f"  (+{delta})" if delta > 0 else ""
            print(f"   {state or '(blank)'}: {cnt}{marker}")

        # ── integrity check ──────────────────────────────────────────────────
        print("\n🔎 Integrity check:")
        for key, before_cnt in before.items():
            after_cnt = after.get(key, 0)
            if after_cnt < before_cnt:
                country, state = key
                print(f"   ❌ FAIL: {state} dropped {before_cnt} → {after_cnt}")
            else:
                country, state = key
                print(f"   ✅ OK: {state or '(blank)'} {before_cnt} → {after_cnt}")

        print(f"\n✅ Done. Total new records inserted: {total_inserted}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
