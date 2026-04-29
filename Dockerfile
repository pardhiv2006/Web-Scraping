# ── Stage: Production image ──────────────────────────────────────────────────
FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    curl \
    chromium \
    chromium-driver \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1001 appuser

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY businesses.db ./

# Permissions
RUN chown -R appuser:appuser /app
USER appuser

# Switch to backend
WORKDIR /app/backend

# Env variables
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromium-driver
ENV PYTHONUNBUFFERED=1

# ⚠️ IMPORTANT: expose Render port
EXPOSE 10000

# Healthcheck (use fixed port)
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:10000/health || exit 1

# ✅ Start server (NO fallback, force Render PORT)
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT"]
