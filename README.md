[README.md](https://github.com/user-attachments/files/21906672/README.md)
# Weather Data API — Take‑Home Solution

This project ingests daily weather observations from fixed-width/tab-separated station files,
stores them in SQLite via SQLAlchemy, and exposes a REST API with pagination, filtering,
and an OpenAPI/Swagger UI.

## Features

- **Problem 1 — Data Modeling**
  - Tables: `weather_stations`, `weather_observations`
  - Unique constraint prevents duplicate `(station_id, date)` rows
  - Indexes for fast filtering

- **Problem 2 — Ingestion**
  - `ingest.py` parses station files in a directory
  - Converts units from tenths (as provided) into SI units
  - Skips missing values `-9999` and deduplicates with UPSERT
  - Structured logging: start/end times & rows ingested

- **Problem 3 — Data Analysis & API**
  - `GET /api/weather` — raw observations with filters & pagination
  - `GET /api/weather/stats` — per-year, per-station stats:
    - average max temp (°C), average min temp (°C), total precipitation (mm)
  - Query params: `station_id`, `date_from`, `date_to`, `year`, `page`, `page_size`
  - OpenAPI docs at `/apidocs` and `/openapi.json`

- **Tests**
  - `pytest` covers ingestion and API paths

## Quickstart

```bash
# 1) Create a virtualenv and install deps
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2) Ingest sample data into SQLite
python ingest.py --data-dir data/sample --database sqlite:///weather.db

# 3) Run the API
FLASK_APP=app.web:app flask run  # then open http://127.0.0.1:5000/apidocs

# Alternatively:
python -m app.web
```

## Endpoints

- `GET /api/health` — service health
- `GET /api/weather` — raw observations (filter by `station_id`, date range)
- `GET /api/weather/stats` — yearly per-station aggregates
- `GET /openapi.json`, `GET /apidocs` — OpenAPI & Swagger UI

## CLI

```bash
python ingest.py --help
```

## Notes

- Missing values are `-9999` in source. We store `NULL` for those fields.
- Units conversion:
  - temperatures in tenths of °C → divide by 10.0
  - precipitation in tenths of mm → divide by 10.0
- Idempotency: ingestion uses `INSERT ... ON CONFLICT DO UPDATE` (SQLite UPSERT).
- Pagination default: `page=1`, `page_size=100`, max page size 1000.
