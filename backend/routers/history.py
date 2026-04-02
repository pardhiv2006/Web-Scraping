import json
from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional

from database import get_db
from models.search_history import SearchHistory
from services.security import decode_token

router = APIRouter(prefix="/history", tags=["Search History"])

class HistorySave(BaseModel):
    country: str
    states: List[str]
    result_count: int

def get_current_user_id(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.split(" ")[1]
    payload = decode_token(token)
    return payload.get("id") if payload else None

@router.get("")
def get_history(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    user_id = get_current_user_id(authorization)
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    history = db.query(SearchHistory).filter(SearchHistory.user_id == user_id).order_by(SearchHistory.searched_at.desc()).all()
    return [h.to_dict() for h in history]

@router.post("")
def save_history(data: HistorySave, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    user_id = get_current_user_id(authorization)
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    new_history = SearchHistory(
        user_id=user_id,
        country=data.country,
        states=json.dumps(data.states),
        result_count=data.result_count
    )
    db.add(new_history)
    db.commit()
    db.refresh(new_history)
    return new_history.to_dict()

@router.delete("/{history_id}")
def delete_history(history_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    user_id = get_current_user_id(authorization)
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    item = db.query(SearchHistory).filter(SearchHistory.id == history_id, SearchHistory.user_id == user_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="History item not found")
    
    db.delete(item)
    db.commit()
    return {"success": True}
