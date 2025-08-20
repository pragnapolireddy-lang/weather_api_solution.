from __future__ import annotations
import argparse
import logging
from pathlib import Path
from datetime import datetime
from dateutil import parser as dtparser
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from app.db import make_engine, make_session_factory, init_db
from app.models import WeatherStation, WeatherObservation
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")

MISSING = -9999

def parse_line(line: str):
    # Expect 4 tab-separated fields: yyyymmdd, tmax(0.1C), tmin(0.1C), prcp(0.1mm)
    parts = line.strip().split("\t")
    if len(parts) != 4:
        raise ValueError(f"Expected 4 fields, got {len(parts)}: {line!r}")
    yyyymmdd, raw_tmax, raw_tmin, raw_prcp = parts
    dt = datetime.strptime(yyyymmdd, "%Y%m%d").date()
    def conv(v):
        iv = int(v)
        if iv == MISSING:
            return None
        return iv / 10.0
    return dt, conv(raw_tmax), conv(raw_tmin), conv(raw_prcp)

def ingest_station_file(session, station_id: str, path: Path) -> int:
    inserted = 0
    # ensure station row
    session.merge(WeatherStation(id=station_id))

    with path.open() as f:
        for line in f:
            if not line.strip():
                continue
            dt, tmax, tmin, prcp = parse_line(line)
            # Upsert via ON CONFLICT (SQLite) by unique (station_id, date)
            session.execute(
                text(
                    """
                    INSERT INTO weather_observations (station_id, date, tmax_c, tmin_c, prcp_mm)
                    VALUES (:station_id, :date, :tmax, :tmin, :prcp)
                    ON CONFLICT(station_id, date) DO UPDATE SET
                        tmax_c = excluded.tmax_c,
                        tmin_c = excluded.tmin_c,
                        prcp_mm = excluded.prcp_mm
                    """
                ),
                {
                    "station_id": station_id,
                    "date": dt.isoformat(),
                    "tmax": tmax,
                    "tmin": tmin,
                    "prcp": prcp,
                },
            )
            inserted += 1
    return inserted

def main():
    ap = argparse.ArgumentParser(description="Ingest weather data files into the database.")
    ap.add_argument("--data-dir", required=True, help="Directory containing station files (e.g., USC0001.txt)")
    ap.add_argument("--database", default="sqlite:///weather.db", help="Database URL (e.g., sqlite:///weather.db)")
    args = ap.parse_args()

    engine = make_engine(args.database)
    init_db(engine)
    Session = make_session_factory(engine)

    start = datetime.utcnow()
    total = 0
    with Session() as session:
        for path in Path(args.data_dir).glob("*.txt"):
            station_id = path.stem  # filename without extension
            log.info("Ingesting %s from %s", station_id, path)
            total += ingest_station_file(session, station_id, path)
        session.commit()
    end = datetime.utcnow()
    log.info("Ingestion complete. Records processed: %d. Start: %s End: %s", total, start.isoformat(), end.isoformat())

if __name__ == "__main__":
    main()
