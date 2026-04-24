from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db
from models.business import Business

router = APIRouter(tags=["Countries"])


@router.get("/countries")
def get_countries(db: Session = Depends(get_db)):
    """Return list of distinct countries from database."""
    countries = db.query(Business.country).distinct().all()
    # Filter out None and empty strings
    country_list = [{"code": c[0], "name": c[0]} for c in countries if c[0] and c[0].strip()]
    return {"countries": sorted(country_list, key=lambda x: x['name'])}


@router.get("/countries/{country_code}/states")
def get_states(country_code: str, db: Session = Depends(get_db)):
    """Return available states/regions for a given country."""
    code = country_code.upper()
    states = db.query(Business.state).filter(Business.country == code).distinct().all()
    # Filter out None and empty strings
    state_list = [{"code": s[0], "name": s[0]} for s in states if s[0] and s[0].strip()]
    return {"country": code, "states": sorted(state_list, key=lambda x: x['name'])}
