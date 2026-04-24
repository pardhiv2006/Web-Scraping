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

    country_upper = country.strip().upper() if country else None
    states_upper = [s.strip().upper() for s in state] if state else None
    logger.info(f"GET /businesses → country={country_upper}, state={states_upper}, page={page}, strict={strict}")

    if country_upper:
        query = query.filter(func.upper(Business.country) == country_upper)
    if states_upper:
        query = query.filter(func.upper(Business.state).in_(states_upper))

    if strict:
        # We now assume the database is kept clean via strict_cleanup.py 
        # and other ingestion-time checks. Removing runtime slow filters.
        pass

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
