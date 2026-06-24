# ── Base image ────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# ── Working directory ─────────────────────────────────────────────────────────
WORKDIR /app

# ── System build dependencies ─────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ───────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Project source ────────────────────────────────────────────────────────────
COPY . .

# ── Pre-create output directories ─────────────────────────────────────────────
RUN mkdir -p data/raw data/processed data/curated data/predictions \
             warehouse reports logs models mlruns

# ── Expose ports ─────────────────────────────────────────────────────────────
# 8011 — FastAPI   (hdb-api service)
# 8501 — Streamlit (hdb-dashboard service)
EXPOSE 8011
EXPOSE 8501

# ── Default command ───────────────────────────────────────────────────────────
# Override in docker-compose.yml per service.
CMD ["python", "-m", "src.run_pipeline"]
