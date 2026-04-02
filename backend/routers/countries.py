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
        {"code": "AL", "name": "Alabama"}, {"code": "AK", "name": "Alaska"},
        {"code": "AZ", "name": "Arizona"}, {"code": "AR", "name": "Arkansas"},
        {"code": "CA", "name": "California"}, {"code": "CO", "name": "Colorado"},
        {"code": "CT", "name": "Connecticut"}, {"code": "DE", "name": "Delaware"},
        {"code": "FL", "name": "Florida"}, {"code": "GA", "name": "Georgia"},
        {"code": "HI", "name": "Hawaii"}, {"code": "ID", "name": "Idaho"},
        {"code": "IL", "name": "Illinois"}, {"code": "IN", "name": "Indiana"},
        {"code": "IA", "name": "Iowa"}, {"code": "KS", "name": "Kansas"},
        {"code": "KY", "name": "Kentucky"}, {"code": "LA", "name": "Louisiana"},
        {"code": "ME", "name": "Maine"}, {"code": "MD", "name": "Maryland"},
        {"code": "MA", "name": "Massachusetts"}, {"code": "MI", "name": "Michigan"},
        {"code": "MN", "name": "Minnesota"}, {"code": "MS", "name": "Mississippi"},
        {"code": "MO", "name": "Missouri"}, {"code": "MT", "name": "Montana"},
        {"code": "NE", "name": "Nebraska"}, {"code": "NV", "name": "Nevada"},
        {"code": "NH", "name": "New Hampshire"}, {"code": "NJ", "name": "New Jersey"},
        {"code": "NM", "name": "New Mexico"}, {"code": "NY", "name": "New York"},
        {"code": "NC", "name": "North Carolina"}, {"code": "ND", "name": "North Dakota"},
        {"code": "OH", "name": "Ohio"}, {"code": "OK", "name": "Oklahoma"},
        {"code": "OR", "name": "Oregon"}, {"code": "PA", "name": "Pennsylvania"},
        {"code": "RI", "name": "Rhode Island"}, {"code": "SC", "name": "South Carolina"},
        {"code": "SD", "name": "South Dakota"}, {"code": "TN", "name": "Tennessee"},
        {"code": "TX", "name": "Texas"}, {"code": "UT", "name": "Utah"},
        {"code": "VT", "name": "Vermont"}, {"code": "VA", "name": "Virginia"},
        {"code": "WA", "name": "Washington"}, {"code": "WV", "name": "West Virginia"},
        {"code": "WI", "name": "Wisconsin"}, {"code": "WY", "name": "Wyoming"},
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
        {"code": "AJM", "name": "Ajman"},
        {"code": "RAK", "name": "Ras Al Khaimah"},
        {"code": "FUJ", "name": "Fujairah"},
        {"code": "UAQ", "name": "Umm Al Quwain"},
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
