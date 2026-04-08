"""
Countries and states/regions router.
"""
from fastapi import APIRouter

router = APIRouter(tags=["Countries"])

COUNTRIES = [
    {"code": "US", "name": "USA"},
    {"code": "UK", "name": "UK"},
    {"code": "UAE", "name": "UAE"},
]


STATES = {
    "US": [
        {"code": "CA", "name": "California"},
        {"code": "TX", "name": "Texas"},
        {"code": "FL", "name": "Florida"},
        {"code": "NY", "name": "New York"},
        {"code": "IL", "name": "Illinois"},
        {"code": "PA", "name": "Pennsylvania"},
        {"code": "OH", "name": "Ohio"},
        {"code": "GA", "name": "Georgia"},
        {"code": "NC", "name": "North Carolina"},
        {"code": "MI", "name": "Michigan"},
        {"code": "NJ", "name": "New Jersey"},
        {"code": "VA", "name": "Virginia"},
        {"code": "WA", "name": "Washington"},
        {"code": "AZ", "name": "Arizona"},
        {"code": "MA", "name": "Massachusetts"},
    ],
    "UK": [
        {"code": "ENG", "name": "England"},
        {"code": "SCT", "name": "Scotland"},
        {"code": "WLS", "name": "Wales"},
        {"code": "NIR", "name": "Northern Ireland"},
    ],
    "UAE": [
        {"code": "DXB", "name": "Dubai"},
        {"code": "AUH", "name": "Abu Dhabi"},
        {"code": "SHJ", "name": "Sharjah"},
    ],
}


@router.get("/countries")
def get_countries():
    """Return list of supported countries."""
    return {"countries": COUNTRIES}


@router.get("/countries/{country_code}/states")
def get_states(country_code: str):
    """Return available states/regions for a given country."""
    code = country_code.upper()
    if code not in STATES:
        return {"error": f"Country '{country_code}' not supported.", "states": []}
    return {"country": code, "states": STATES[code]}
