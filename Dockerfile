# ELT Pipeline - Production image
# Build: docker build -t elt-pipeline .
# Run:   docker run --env-file .env elt-pipeline

FROM python:3.11-slim

WORKDIR /app

# Install system deps if needed (e.g. for duckdb/polars)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Prefer uv for faster installs; fallback to pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY pyproject.toml .

# Default: run full pipeline; override with docker run ... <command>
CMD ["python", "-m", "src.ingestion.main"]
