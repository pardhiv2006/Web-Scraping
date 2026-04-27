from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db
from models.business import Business

router = APIRouter(tags=["Countries"])

# ── Full-name mapping (server-side) ──────────────────────────────────────────
_STATE_DISPLAY = {
    # UAE emirates
    "DXB": "Dubai",
    "AUH": "Abu Dhabi",
    "SHJ": "Sharjah",
    "AJM": "Ajman",
    "RAK": "Ras Al Khaimah",
    "UAQ": "Umm Al Quwain",
    "FUJ": "Fujairah",
    # UK regions
    "SCT": "Scotland",
    "ENG": "England",
    "WLS": "Wales",
    "NIR": "Northern Ireland",
}


@router.get("/countries")
def get_countries(db: Session = Depends(get_db)):
    """Return list of distinct countries from database."""
    countries = db.query(Business.country).distinct().all()
    # Filter out None and empty strings
    country_list = [{"code": c[0], "name": c[0]} for c in countries if c[0] and c[0].strip()]
    return {"countries": sorted(country_list, key=lambda x: x['name'])}


@router.get("/countries/{country_code}/states")
def get_states(country_code: str, db: Session = Depends(get_db)):
    """
    Return available states/regions for a given country with full display names.
    Only returns states that have at least one valid business record (with a name).
    """
    code = country_code.upper()
    
    # Query for distinct states that have at least one valid company name
    states = (
        db.query(Business.state)
        .filter(Business.country == code)
        .filter(Business.company_name.isnot(None))
        .filter(Business.company_name != "")
        .filter(Business.state.isnot(None))
        .filter(Business.state != "")
        .distinct()
        .all()
    )
    
    # Map codes to full names; fall back to raw value if not in map
    state_list = [
        {"code": s[0], "name": _STATE_DISPLAY.get(s[0].upper(), s[0])}
        for s in states if s[0] and s[0].strip()
    ]
    return {"country": code, "states": sorted(state_list, key=lambda x: x['name'])}
