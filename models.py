from sqlalchemy import Column, String, Integer, DateTime, JSON
from sqlalchemy.orm import declarative_base
import datetime

Base = declarative_base()

class Session(Base):
    __tablename__ = "sessions"

    id = Column(String, primary_key=True)
    page_url = Column(String)
    start_time = Column(DateTime, default=datetime.datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    rage_click_count = Column(Integer, default=0)
    device_type = Column(String, nullable=True)

class SessionEvent(Base):
    __tablename__ = "session_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String)
    event_blob = Column(JSON)