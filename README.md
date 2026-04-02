# Business Registry Scraper

A full-stack web application for scraping newly registered business data from public registries in the US, UK, and UAE.

## Features
- **Frontend Dashboard**: Modern glassmorphism UI for controlling target countries and regions
- **FastAPI Backend**: Robust API with SQLite + SQLAlchemy storage
- **Scrapers**: 
  - **US**: OpenCorporates API implementation
  - **UK**: Companies House API implementation 
  - **UAE**: Open Data portal integration
- **Smart Fallbacks**: If API keys aren't provided or public sources rate-limit, the system automatically uses a synthetic data generator to simulate realistic business registries based on the selected region.
- **Export**: Built-in CSV exporting

## Quick Start (Local Run)

### 1. Backend Setup
If you have Node.js installed, you can easily install and run the application using npm from the root folder:
```bash
# First time only - install python dependencies
npm run install:backend

# Start the application
npm run dev
```

Alternatively, you can run it purely with Python:
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### 2. Access the Application
Open your browser and navigate to:
[http://localhost:8000/](http://localhost:8000/)

*Note: The FastAPI app is configured to act as a static file server for the `frontend/` directory, so you don't need a separate frontend server.*

## API Endpoints
- `GET /api/countries` - List supported countries
- `GET /api/countries/{code}/states` - Get regions for a country
- `POST /api/scrape` - Trigger scraping job
- `GET /api/businesses` - Retrieve scraped records
- `GET /api/export/csv` - Download data as CSV

## Configuration
See `.env.example` to configure optional API keys for real-world scraping.
