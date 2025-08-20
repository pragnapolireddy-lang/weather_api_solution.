from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base

def make_engine(database_url: str):
    engine = create_engine(database_url, future=True)
    return engine

def make_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def init_db(engine):
    Base.metadata.create_all(engine)
