"""
Scrape trigger router — POST /api/scrape
"""
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List
from sqlalchemy.orm import Session

from database import get_db
from services.scrape_service import run_scrape

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Scrape"])


class ScrapeRequest(BaseModel):
    country: str
    states: List[str]


@router.post("/scrape")
def start_scrape(request: ScrapeRequest, db: Session = Depends(get_db)):
    """
    Trigger the scraping process for selected country and states.
    Returns a summary of records inserted, skipped, and errors.
    """
    if not request.country:
        raise HTTPException(status_code=400, detail="Country is required.")
    if not request.states:
        raise HTTPException(status_code=400, detail="At least one state must be selected.")

    logger.info(f"Scrape requested: country={request.country}, states={request.states}")

    try:
        result = run_scrape(
            country=request.country.upper(),
            states=request.states,
            db=db,
        )
        return {"success": True, "summary": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")
