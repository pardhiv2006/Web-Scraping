# ── Stage: Production image ──────────────────────────────────────────────────
FROM python:3.11-slim

# System deps for aiohttp, beautifulsoup4, lxml/extruct, trafilatura
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN useradd -m -u 1001 appuser

# Working directory
WORKDIR /app

# ── Install Python dependencies ───────────────────────────────────────────────
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy application code ─────────────────────────────────────────────────────
# Backend
COPY backend/ ./backend/
# Frontend (static files served by FastAPI)
COPY frontend/ ./frontend/

# ── Permissions ───────────────────────────────────────────────────────────────
RUN chown -R appuser:appuser /app
USER appuser

# ── Runtime configuration ─────────────────────────────────────────────────────
WORKDIR /app/backend

# Expose the port FastAPI listens on
EXPOSE 8000

# Health check so container orchestrators know when the app is ready
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start the server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
