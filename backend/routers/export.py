import io
import csv
import logging
from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session
from typing import Optional, List

from database import get_db
from models.business import Business

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Export"])


@router.get("/export/csv")
def export_csv(
    country: Optional[str] = Query(None),
    state: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Export stored business records as a downloadable CSV file.
    Optionally filter by country and/or multiple states.
    """
    query = db.query(Business)

    country_upper = country.strip().upper() if country else None
    states_upper = [s.strip().upper() for s in state] if state else None
    logger.info(f"CSV Export → country={country_upper}, states={states_upper}")

    if country_upper:
        query = query.filter(Business.country == country_upper)
    if states_upper:
        query = query.filter(Business.state.in_(states_upper))


    records = query.order_by(Business.scraped_at.desc()).all()
    logger.info(f"Exporting {len(records)} records. Filters: country={country_upper}, states={states_upper}")

    output = io.StringIO()
    writer = csv.writer(output)

    # Header row
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
        "CEO Name",
        "Founder Name",
        "CEO Email",
        "LinkedIn Profile",
    ])

    # Data rows
    for b in records:
        # Format links for Excel compatibility using the robust formula format: ="=HYPERLINK(""url"",""label"")"
        # This ensures they are clickable and use the same labels as the Web UI.
        
        website_link = ""
        if b.website:
            url = b.website if b.website.startswith(("http://", "https://")) else "https://" + b.website
            # Format label similar to Web UI (domain + arrow)
            label = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0] + " ⇗"
            # Standard Excel formula format without extra space or redundant outer quoting
            website_link = f'=HYPERLINK("{url}","{label}")'

        linkedin_link = ""
        if b.linkedin_url:
            url = b.linkedin_url if b.linkedin_url.startswith(("http://", "https://")) else "https://" + b.linkedin_url
            label = "LinkedIn ⇗"
            linkedin_link = f'=HYPERLINK("{url}","{label}")'
        
        writer.writerow([
            b.company_name,
            b.registration_number,
            b.registration_date or "",
            b.status or "",
            b.address or "",
            b.country,
            b.state or "",
            b.email or "",
            b.phone or "",
            website_link,
            b.ceo_name or "",
            b.founder_name or "",
            b.ceo_email or "",
            linkedin_link,
        ])

    content = output.getvalue()
    # Add UTF-8 BOM for better Excel compatibility
    bom = "\ufeff"
    
    return Response(
        content=bom + content,
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=businesses.csv",
            "Pragma": "no-cache",
            "Expires": "0",
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "X-Content-Type-Options": "nosniff"
        },
    )
