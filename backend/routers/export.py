import io
import csv
import json
import logging
from datetime import datetime
from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session
from typing import Optional, List

from database import get_db
from models.business import Business
from models.search_history import SearchHistory

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Export"])


@router.get("/export/csv")
def export_csv(
    country: Optional[str] = Query(None),
    state: Optional[List[str]] = Query(None),
    history_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Export stored business records as a downloadable CSV file.
    Exact Record Count Guarantee: 
    - If history_id is provided, exports the EXACT snapshot seen in History UI.
    - Otherwise, exports all records matching the current country/state filters.
    """
    records = []
    
    if history_id:
        history_item = db.query(SearchHistory).filter(SearchHistory.id == history_id).first()
        if history_item and history_item.result_data:
            records = json.loads(history_item.result_data)
            logger.info(f"CSV Export → Using History Snapshot ID {history_id} ({len(records)} records)")
    else:
        query = db.query(Business)
        from services.enrichment_service import normalize_country
        country_norm = normalize_country(country)
        states_upper = [s.strip().upper() for s in state] if state else None
        logger.info(f"CSV Export → Live Query: country={country_norm}, states={states_upper}")


        from sqlalchemy import func
        if country_norm:
            query = query.filter(func.upper(Business.country) == country_norm.upper())
        if states_upper:
            query = query.filter(func.upper(Business.state).in_(states_upper))


        db_records = query.order_by(Business.scraped_at.desc()).all()
        records = [b.to_dict() for b in db_records]
        logger.info(f"CSV Export → Exporting {len(records)} live records.")

    output = io.StringIO()
    writer = csv.writer(output)

    # Header row - Extended to include Source URL as per requirement
    writer.writerow([
        "Company Name",
        "Reg Number",
        "Reg Date",
        "Status",
        "Address",
        "Country",
        "State",
        "Email",
        "Phone",
        "Website",
        "LinkedIn Profile",
        "CEO Name",
        "Founder Name",
        "CEO Email",
        "Source URL (View Link)"
    ])

    def clean_url(url):
        if not url: return ""
        url = str(url).strip()
        if not url.startswith(("http://", "https://")):
            return "https://" + url
        return url

    # Data rows
    for b in records:
        # Compatibility helper for both dict (from history) and object (from live query)
        def g(field):
            return b.get(field) if isinstance(b, dict) else getattr(b, field, "")

        # Format links for Excel compatibility using robust HYPERLINK formula
        website_url = clean_url(g("website"))
        website_link = f'=HYPERLINK("{website_url}","Website ⇗")' if website_url else ""

        linkedin_url = clean_url(g("linkedin_url"))
        linkedin_link = f'=HYPERLINK("{linkedin_url}","LinkedIn ⇗")' if linkedin_url else ""

        source_url = clean_url(g("source_url"))
        source_link = f'=HYPERLINK("{source_url}","View Source ⇗")' if source_url else ""
        
        writer.writerow([
            g("company_name"),
            g("registration_number"),
            g("registration_date") or "Not Available",
            g("status") or "Active",
            g("address") or "Not Available",
            g("country"),
            g("state") or "Not Available",
            g("email") or "Not Available",
            g("phone") or "Not Available",
            website_link,
            linkedin_link,
            g("ceo_name") or "Not Available",
            g("founder_name") or "Not Available",
            g("ceo_email") or "Not Available",
            source_link,
        ])

    content = output.getvalue()
    bom = "\ufeff" # UTF-8 BOM for Excel
    
    return Response(
        content=bom + content,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=businesses_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            "Pragma": "no-cache",
            "Expires": "0",
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        },
    )
