"""
FastAPI application entry point.
"""
import os
import sys

# Ensure backend directory is on the path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse


from database import init_db
from routers import countries, scrape, businesses, export, auth, history

app = FastAPI(
    title="Business Registry Scraper",
    description="Scrapes newly registered business data from US, UK, and UAE",
    version="1.0.0",
)

# CORS middleware — allows all origins (tighten in prod if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Path Configuration ---
backend_dir = os.path.dirname(os.path.abspath(__file__))
# Works both locally and in Docker (frontend/ is always one level up from backend/)
frontend_dir = os.path.abspath(os.path.join(backend_dir, "..", "frontend"))

# =============================================================================
# API Routes (MUST be registered before any static/catch-all mounts)
# =============================================================================
app.include_router(countries.router, prefix="/api")
app.include_router(scrape.router, prefix="/api")
app.include_router(businesses.router, prefix="/api")
app.include_router(export.router, prefix="/api")
app.include_router(auth.router, prefix="/api")
app.include_router(history.router, prefix="/api")


@app.get("/health")
def health():
    return {"status": "ok", "message": "Business Registry Scraper is running"}


# =============================================================================
# Frontend Page Routes
# =============================================================================

@app.get("/")
def serve_index():
    """Serve the main dashboard page."""
    return FileResponse(os.path.join(frontend_dir, "index.html"))


@app.get("/login.html")
@app.get("/login")
def serve_login():
    """Serve the login page via both /login and /login.html."""
    return FileResponse(os.path.join(frontend_dir, "login.html"))


# =============================================================================
# Static Assets (CSS, JS, images)
# Mounted AFTER named routes so /api/* and /health are not shadowed
# =============================================================================
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="frontend-static")
else:
    print(f"WARNING: frontend directory not found at {frontend_dir}")


# =============================================================================
# Catch-all — serves index.html for any unrecognized path so refreshing works
# =============================================================================
@app.get("/{full_path:path}")
async def catch_all(full_path: str):
    """Return the requested static file if it exists, else fall back to index.html."""
    requested = os.path.join(frontend_dir, full_path)
    if os.path.isfile(requested):
        return FileResponse(requested)
    # Fall back to SPA entry point
    return FileResponse(os.path.join(frontend_dir, "index.html"))


# =============================================================================
# Startup
# =============================================================================
@app.on_event("startup")
def on_startup():
    init_db()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
