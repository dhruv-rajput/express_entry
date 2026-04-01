# ── Stage 1: Build dependencies ──────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# System dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies into a separate directory (keeps final image clean)
COPY requirements.txt .

# Skip playwright browser download (saves ~1.5GB) — we only use it for HTTP scraping, not browser automation
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Final slim image ─────────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# Only runtime system dependencies (no build tools)
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy application code
COPY . .

# Skip playwright browsers (not needed — we use httpx/beautifulsoup for scraping)
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# SERVICE_TYPE controls what process runs:
#   api          → FastAPI server (default)
#   worker       → Celery worker
#   beat         → Celery beat scheduler
ENV SERVICE_TYPE=api

CMD ["sh", "-c", "\
  if [ \"$SERVICE_TYPE\" = \"worker\" ]; then \
    exec celery -A workers.tasks.celery_app worker --loglevel=info --concurrency=2 -Q celery; \
  elif [ \"$SERVICE_TYPE\" = \"beat\" ]; then \
    exec celery -A workers.tasks.celery_app beat --loglevel=info --scheduler celery.beat:PersistentScheduler; \
  else \
    exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}; \
  fi \
"]
