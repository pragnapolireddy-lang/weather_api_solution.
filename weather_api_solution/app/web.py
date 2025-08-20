from __future__ import annotations
import math
from datetime import date, datetime
from typing import Optional, Tuple
from flask import Flask, jsonify, request
from flasgger import Swagger
from sqlalchemy import func, select, and_
from sqlalchemy.orm import Session
from .db import make_engine, make_session_factory, init_db
from .models import WeatherObservation, WeatherStation

DEFAULT_DB = "sqlite:///weather.db"

def create_app(database_url: str = DEFAULT_DB) -> Flask:
    app = Flask(__name__)
    app.config["SWAGGER"] = {
        "title": "Weather API",
        "uiversion": 3,
    }
    Swagger(app)

    engine = make_engine(database_url)
    init_db(engine)
    SessionFactory = make_session_factory(engine)

    def paginate(query, page: int, page_size: int, session: Session):
        total = session.execute(select(func.count()).select_from(query.subquery())).scalar_one()
        items = session.execute(query.limit(page_size).offset((page - 1) * page_size)).all()
        return total, [dict(row._mapping) for row in items]

    @app.get("/api/health")
    def health():
        return jsonify({"status": "ok", "timestamp": datetime.utcnow().isoformat()})

    @app.get("/api/weather")
    def get_weather():
        """List weather observations.
        ---
        parameters:
          - name: station_id
            in: query
            schema: {type: string}
          - name: date_from
            in: query
            schema: {type: string, format: date}
          - name: date_to
            in: query
            schema: {type: string, format: date}
          - name: page
            in: query
            schema: {type: integer, default: 1}
          - name: page_size
            in: query
            schema: {type: integer, default: 100}
        responses:
          200:
            description: A page of observations
        """
        station_id = request.args.get("station_id")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")
        page = max(1, int(request.args.get("page", 1)))
        page_size = min(1000, max(1, int(request.args.get("page_size", 100))))

        with SessionFactory() as session:
            obs = select(
                WeatherObservation.station_id,
                WeatherObservation.date,
                WeatherObservation.tmax_c,
                WeatherObservation.tmin_c,
                WeatherObservation.prcp_mm,
            )
            cond = []
            if station_id:
                cond.append(WeatherObservation.station_id == station_id)
            if date_from:
                cond.append(WeatherObservation.date >= date.fromisoformat(date_from))
            if date_to:
                cond.append(WeatherObservation.date <= date.fromisoformat(date_to))
            if cond:
                obs = obs.where(and_(*cond)).order_by(WeatherObservation.station_id, WeatherObservation.date)
            else:
                obs = obs.order_by(WeatherObservation.station_id, WeatherObservation.date)

            total, rows = paginate(obs, page, page_size, session)
            return jsonify({"page": page, "page_size": page_size, "total": total, "items": rows})

    @app.get("/api/weather/stats")
    def get_stats():
        """Yearly per-station aggregates.
        ---
        parameters:
          - name: station_id
            in: query
            schema: {type: string}
          - name: year
            in: query
            schema: {type: integer}
          - name: page
            in: query
            schema: {type: integer, default: 1}
          - name: page_size
            in: query
            schema: {type: integer, default: 100}
        responses:
          200:
            description: A page of aggregated statistics
        """
        station_id = request.args.get("station_id")
        year = request.args.get("year", type=int)
        page = max(1, int(request.args.get("page", 1)))
        page_size = min(1000, max(1, int(request.args.get("page_size", 100))))

        with SessionFactory() as session:
            q = select(
                WeatherObservation.station_id.label("station_id"),
                func.strftime('%Y', WeatherObservation.date).label("year"),
                func.avg(WeatherObservation.tmax_c).label("avg_tmax_c"),
                func.avg(WeatherObservation.tmin_c).label("avg_tmin_c"),
                func.sum(WeatherObservation.prcp_mm).label("total_prcp_mm"),
            ).group_by(WeatherObservation.station_id, func.strftime('%Y', WeatherObservation.date))

            cond = []
            if station_id:
                cond.append(WeatherObservation.station_id == station_id)
            if year is not None:
                cond.append(func.strftime('%Y', WeatherObservation.date) == str(year))
            if cond:
                q = q.where(and_(*cond))
            q = q.order_by("station_id", "year")

            total, rows = paginate(q, page, page_size, session)
            return jsonify({"page": page, "page_size": page_size, "total": total, "items": rows})

    return app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
