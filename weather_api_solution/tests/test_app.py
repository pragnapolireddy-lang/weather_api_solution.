import os
import tempfile
from pathlib import Path
from app.web import create_app
from app.db import make_engine, init_db

def write_sample(tmpdir: Path):
    # Two days, with one missing value
    data = "20200101\t250\t100\t12\n20200102\t-9999\t50\t0\n"
    path = tmpdir / "TESTSTATION.txt"
    path.write_text(data)
    return path

def test_ingest_and_api(tmp_path):
    from ingest import ingest_station_file
    from app.db import make_session_factory
    from app.models import WeatherObservation

    db_url = f"sqlite:///{tmp_path}/t.db"
    engine = make_engine(db_url)
    init_db(engine)
    Session = make_session_factory(engine)

    # ingest
    with Session() as s:
        p = write_sample(tmp_path)
        n = ingest_station_file(s, "TESTSTATION", p)
        s.commit()
        assert n == 2

    # api
    app = create_app(db_url)
    client = app.test_client()
    r = client.get("/api/weather?station_id=TESTSTATION")
    assert r.status_code == 200
    data = r.get_json()
    assert data["total"] == 2

    r2 = client.get("/api/weather/stats?station_id=TESTSTATION&year=2020")
    assert r2.status_code == 200
    stats = r2.get_json()
    assert stats["total"] == 1
