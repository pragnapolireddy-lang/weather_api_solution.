from __future__ import annotations
from datetime import date
from sqlalchemy import (
    Column, String, Integer, Float, Date, ForeignKey, UniqueConstraint, Index, text
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class WeatherStation(Base):
    __tablename__ = "weather_stations"
    id = Column(String(32), primary_key=True)  # station id from filename
    name = Column(String(255), nullable=True)  # optional (not always provided)

    observations = relationship("WeatherObservation", back_populates="station", cascade="all, delete-orphan")

class WeatherObservation(Base):
    __tablename__ = "weather_observations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String(32), ForeignKey("weather_stations.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    tmax_c = Column(Float, nullable=True)
    tmin_c = Column(Float, nullable=True)
    prcp_mm = Column(Float, nullable=True)

    station = relationship("WeatherStation", back_populates="observations")

    __table_args__ = (
        UniqueConstraint("station_id", "date", name="uq_station_date"),
        Index("ix_obs_station_date", "station_id", "date"),
        Index("ix_obs_date", "date"),
    )
