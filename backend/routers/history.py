"""
Search History router.

Endpoints:
  GET    /api/history            → list all history items for current user (no result_data)
  POST   /api/history            → save a new history entry with full result_data
  GET    /api/history/{id}       → get single history item INCLUDING result_data
  DELETE /api/history/{id}       → permanently delete a single history entry
  DELETE /api/history            → permanently delete ALL history for current user
"""
import json
import hashlib
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional, Any

from database import get_db
from models.search_history import SearchHistory
from services.security import decode_token

import logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/history", tags=["Search History"])


# ─────────────────────────── Schemas ───────────────────────────

class HistorySave(BaseModel):
    country: str
    states: List[str]
    result_count: int
    result_data: Optional[List[Any]] = None   # full business record list
    pagination_meta: Optional[dict] = None     # total_pages, limit, etc.


# ─────────────────────────── Auth helper ───────────────────────

def get_current_user_id(authorization: Optional[str] = Header(None)) -> Optional[int]:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.split(" ", 1)[1]
    payload = decode_token(token)
    return payload.get("id") if payload else None


def _require_user(authorization: Optional[str] = Header(None)) -> int:
    uid = get_current_user_id(authorization)
    if not uid:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return uid


def _make_search_key(country: str, states: List[str]) -> str:
    """Stable hash of country + sorted state list for deduplication."""
    raw = f"{country.upper()}:{'|'.join(sorted(s.upper() for s in states))}"
    return hashlib.sha1(raw.encode()).hexdigest()


# ─────────────────────────── Routes ────────────────────────────

@router.get("")
def get_history(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """Return all history items for the authenticated user (metadata only, no blob)."""
    user_id = _require_user(authorization)
    history = (
        db.query(SearchHistory)
        .filter(SearchHistory.user_id == user_id)
        .order_by(SearchHistory.searched_at.desc())
        .all()
    )
    logger.info(f"Fetching history list for user {user_id}")
    return [h.to_dict(include_data=False) for h in history]


@router.post("")
def save_history(
    data: HistorySave,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Save a new search history entry.
    Duplicate guard: if an identical search (same country + states)
    was saved within the last 5 minutes, return the existing entry
    instead of creating a duplicate.
    """
    user_id = _require_user(authorization)

    search_key = _make_search_key(data.country, data.states)

    # Note: We removed the 5-minute deduplication guard to ensure every scrape
    # creates a unique, immutable entry in history as requested.

    new_history = SearchHistory(
        user_id=user_id,
        country=data.country.upper(),
        states=json.dumps(data.states),
        result_count=data.result_count,
        result_data=json.dumps(data.result_data) if data.result_data is not None else None,
        pagination_meta=json.dumps(data.pagination_meta) if data.pagination_meta is not None else None,
        search_key=search_key,
    )
    db.add(new_history)
    db.commit()
    db.refresh(new_history)
    logger.info(f"HISTORY: Created entry {new_history.id} for user {user_id} ({data.country})")
    return new_history.to_dict(include_data=False)


@router.put("/{history_id}")
def update_history_item(
    history_id: int,
    data: HistorySave,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Update an existing history entry with enriched data.
    Ensures that as enrichment finishes, the history snapshot is finalized.
    """
    user_id = _require_user(authorization)

    item = (
        db.query(SearchHistory)
        .filter(SearchHistory.id == history_id, SearchHistory.user_id == user_id)
        .first()
    )
    if not item:
        raise HTTPException(status_code=404, detail="History item not found")

    item.result_count = data.result_count
    if data.result_data is not None:
        item.result_data = json.dumps(data.result_data)
    if data.pagination_meta is not None:
        item.pagination_meta = json.dumps(data.pagination_meta)

    db.commit()
    db.refresh(item)
    logger.info(f"HISTORY: Updated entry {history_id} with {len(data.result_data) if data.result_data else 0} records")
    return item.to_dict(include_data=False)


@router.get("/{history_id}")
def get_history_item(
    history_id: int,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Retrieve a single history entry WITH its full cached result_data.
    Ownership is enforced — users cannot access each other's history.
    """
    user_id = _require_user(authorization)

    item = (
        db.query(SearchHistory)
        .filter(SearchHistory.id == history_id, SearchHistory.user_id == user_id)
        .first()
    )
    if not item:
        raise HTTPException(status_code=404, detail="History item not found")

    result = item.to_dict(include_data=True)
    logger.info(f"HISTORY: Detailed retrieval for ID {history_id} (count={len(result.get('result_data') or [])})")

    # Edge-case: data is missing or was never saved
    if result.get("result_data") is None and not result.get("data_error"):
        result["data_error"] = "Data not available"

    return result


@router.delete("")
def clear_all_history(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """Permanently delete ALL history entries for the current user."""
    user_id = _require_user(authorization)

    deleted = (
        db.query(SearchHistory)
        .filter(SearchHistory.user_id == user_id)
        .delete(synchronize_session=False)
    )
    db.commit()
    return {"success": True, "deleted_count": deleted}


@router.delete("/{history_id}")
def delete_history_item(
    history_id: int,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """Permanently delete a single history entry (must belong to current user)."""
    user_id = _require_user(authorization)

    item = (
        db.query(SearchHistory)
        .filter(SearchHistory.id == history_id, SearchHistory.user_id == user_id)
        .first()
    )
    if not item:
        raise HTTPException(status_code=404, detail="History item not found")

    db.delete(item)
    db.commit()
    return {"success": True}
