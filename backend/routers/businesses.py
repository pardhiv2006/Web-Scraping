"""
Businesses listing router — GET /api/businesses
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List
import logging

from sqlalchemy import func
from database import get_db
from models.business import Business

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Businesses"])


@router.get("/businesses")
def get_businesses(
    country: Optional[str] = Query(None),
    state: Optional[List[str]] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=2000),  # Allow up to 2000 for history snapshots
    strict: bool = Query(True),
    db: Session = Depends(get_db),
):
    """
    Retrieve stored business records with optional filtering.
    Supports pagination via page and limit query params.
    """
    query = db.query(Business)

    from services.enrichment_service import normalize_country
    country_norm = normalize_country(country)
    states_upper = [s.strip().upper() for s in state] if state else None
    logger.info(f"GET /businesses → country={country_norm}, state={states_upper}, page={page}, strict={strict}")

    if country_norm:
        query = query.filter(func.upper(Business.country) == country_norm.upper())

    if states_upper:
        # Use shared STATE_MAPPING to ensure query matches DB normalization
        from services.scrape_service import STATE_MAPPING
        norm_states = [STATE_MAPPING.get(s, s).upper() for s in states_upper]
        query = query.filter(func.upper(Business.state).in_(norm_states))

    # Always exclude records with missing core identifying information
    query = query.filter(
        Business.company_name != None, 
        Business.company_name != "",
        Business.address != None,
        Business.address != ""
    )

    if strict:
        # Additional strictness could be added here if needed in the future
        pass

    total = query.count()
    records = (
        query.order_by(Business.company_name.asc(), Business.registration_number.asc())
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
