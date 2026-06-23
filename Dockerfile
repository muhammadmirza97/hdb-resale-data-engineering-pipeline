# ── Base image ────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# ── Working directory ─────────────────────────────────────────────────────────
WORKDIR /app

# ── System build dependencies ─────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ───────────────────────────────────────────────────────
# Copy requirements first so Docker can cache this layer independently.
# The layer is only re-built if requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Project source ────────────────────────────────────────────────────────────
COPY . .

# ── Pre-create output directories ─────────────────────────────────────────────
# These are also created at runtime by paths.ensure_dirs(),
# but pre-creating them avoids permission surprises on first run.
RUN mkdir -p data/raw data/processed data/curated warehouse reports logs

# ── Default command ───────────────────────────────────────────────────────────
# Run the full ELT pipeline when the container starts.
# Override with: docker run <image> python -m src.validate
CMD ["python", "-m", "src.run_pipeline"]
