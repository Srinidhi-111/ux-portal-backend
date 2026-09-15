from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session as DBSession
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from database import engine, get_db
from models import Base, Session, SessionEvent
import datetime

app = FastAPI()

# Allow your frontend (running on localhost:5173) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173",
                   "https://ux-session-replay-analytics.vercel.app"
                   ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create tables on startup if they don't exist yet
Base.metadata.create_all(bind=engine)

@app.get("/api/sessions")
def list_sessions(db: DBSession = Depends(get_db)):
    sessions = db.query(Session).all()
    return sessions

@app.post("/api/sessions/{session_id}/events")
def ingest_events(session_id: str, payload: dict, db: DBSession = Depends(get_db)):
    session = db.get(Session, session_id)
    if not session:
        session = Session(id=session_id, page_url=payload.get("page_url"), rage_click_count=0)
        db.add(session)
        try:
            db.flush()
        except IntegrityError:
            # Another concurrent request already created this session first.
            db.rollback()
            session = db.get(Session, session_id)

    events = payload.get("events", [])
    rage_clicks_in_batch = sum(
        1 for e in events
        if e.get("source") == "custom" and e.get("type") == "rage_click"
    )
    session.rage_click_count += rage_clicks_in_batch

    raw_event = SessionEvent(session_id=session_id, event_blob=events)
    db.add(raw_event)

    db.commit()
    return {"status": "ok", "events_received": len(events)}

@app.get("/api/sessions/{session_id}/replay")
def get_replay_events(session_id: str, db: DBSession = Depends(get_db)):
    raw_batches = (
        db.query(SessionEvent)
        .filter(SessionEvent.session_id == session_id)
        .order_by(SessionEvent.id)
        .all()
    )

    rrweb_events = []
    for batch in raw_batches:
        for item in batch.event_blob:
            if item.get("source") == "rrweb":
                rrweb_events.append(item["event"])

    return rrweb_events