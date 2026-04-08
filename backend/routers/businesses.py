"""
Businesses listing router — GET /api/businesses
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List
import logging

from database import get_db
from models.business import Business

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Businesses"])


@router.get("/businesses")
def get_businesses(
    country: Optional[str] = Query(None),
    state: Optional[List[str]] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Retrieve stored business records with optional filtering.
    Supports pagination via page and limit query params.
    """
    query = db.query(Business)

    country_upper = country.strip().upper() if country else None
    states_upper = [s.strip().upper() for s in state] if state else None
    logger.info(f"GET /businesses → country={country_upper}, state={states_upper}, page={page}")

    if country_upper:
        query = query.filter(Business.country == country_upper)
    if states_upper:
        query = query.filter(Business.state.in_(states_upper))

    # Apply strict 100% completeness filter
    required_string_fields = [
        Business.registration_date, Business.address, Business.status, 
        Business.email, Business.phone, Business.website, 
        Business.ceo_name, Business.ceo_email, Business.founder_name, 
        Business.linkedin_url, Business.industry, Business.description
    ]
    
    placeholders = ['', '-', 'n/a', 'N/A', 'none', 'None', 'null', 'NULL', 'tbd']
    
    for field in required_string_fields:
        query = query.filter(field.isnot(None), field.notin_(placeholders))
        
    # Exclude ranges or fake numbers for exact fields
    query = query.filter(
        Business.employee_count.isnot(None),
        Business.employee_count.notin_(placeholders),
        ~Business.employee_count.like('%-%')
    )
    
    query = query.filter(
        Business.revenue.isnot(None),
        Business.revenue.notin_(placeholders),
        ~Business.revenue.like('%-%')
    )

    total = query.count()
    records = (
        query.order_by(Business.scraped_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "businesses": [b.to_dict() for b in records],
    }
