"""
One-shot cleanup script: removes all placeholder / auto-generated emails and
CEO emails from the existing database records, so the table shows '-' instead
of synthetic values like director@company.com.

A field is considered a placeholder if:
  - Local-part is a known generic prefix (info, director, admin, contact, etc.)
  - Domain is a free/generic provider (gmail.com, etc.)
  - Local-part is a slug of the company name

Run: python clean_placeholder_data.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import re
import logging
from database import SessionLocal
from models.business import Business

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Same rules as smart_scraper._is_placeholder_email ──────────────

PLACEHOLDER_PREFIXES = {
    "info", "contact", "admin", "support", "hello", "no-reply", "noreply",
    "mail", "office", "enquiries", "enquiry", "sales", "marketing",
    "director", "webmaster", "postmaster", "helpdesk", "help",
    "hr", "jobs", "careers", "billing", "accounts", "finance",
    "press", "media", "pr", "legal", "compliance", "privacy",
    "team", "general", "global", "service", "services",
}

GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "protonmail.com", "yandex.com", "mail.com", "zoho.com",
    "example.com", "test.com", "company.com", "email.com",
}


def is_placeholder(email: str, company_name: str = "") -> bool:
    if not email or "@" not in email:
        return True
    local, domain = email.lower().rsplit("@", 1)
    local = local.strip()
    domain = domain.strip()

    if domain in GENERIC_DOMAINS:
        return True
    if local in PLACEHOLDER_PREFIXES:
        return True

    # Check if local part is a slug of the company name
    if company_name:
        slug = re.sub(r"[^a-z0-9]", "", company_name.lower())
        local_clean = re.sub(r"[^a-z0-9]", "", local)
        if slug and local_clean and (slug == local_clean or local_clean == slug):
            return True

    return False


def main():
    db = SessionLocal()
    try:
        businesses = db.query(Business).all()
        logger.info(f"Total records: {len(businesses)}")

        email_cleared = 0
        ceo_email_cleared = 0

        for b in businesses:
            changed = False

            if b.email and is_placeholder(b.email, b.company_name or ""):
                logger.info(f"  Clearing email '{b.email}' from: {b.company_name}")
                b.email = None
                email_cleared += 1
                changed = True

            if b.ceo_email and is_placeholder(b.ceo_email, b.company_name or ""):
                logger.info(f"  Clearing ceo_email '{b.ceo_email}' from: {b.company_name}")
                b.ceo_email = None
                ceo_email_cleared += 1
                changed = True

            # Also clear websites / linkedin that are clearly junk (basic sanity)
            if b.website and not b.website.startswith("http"):
                b.website = "https://" + b.website
                changed = True

        db.commit()
        logger.info(
            f"\nDone. Cleared {email_cleared} company emails, "
            f"{ceo_email_cleared} CEO emails from existing records."
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    main()
