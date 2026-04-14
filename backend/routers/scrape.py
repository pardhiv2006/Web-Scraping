"""
Scrape trigger router — POST /api/scrape
"""
import logging
from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session

from database import get_db
from services.scrape_service import run_scrape
from services.security import decode_token

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Scrape"])


def _get_user_id(authorization: Optional[str] = Header(None)) -> Optional[int]:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.split(" ", 1)[1]
    payload = decode_token(token)
    return payload.get("id") if payload else None


class ScrapeRequest(BaseModel):
    country: str
    states: List[str]


@router.post("/scrape")
def start_scrape(request: ScrapeRequest, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
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
        user_id = _get_user_id(authorization)
        result = run_scrape(
            country=request.country.upper(),
            states=request.states,
            db=db,
            user_id=user_id
        )
        return {"success": True, "summary": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")
