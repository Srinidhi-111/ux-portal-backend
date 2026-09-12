from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session as DBSession
from sqlalchemy import text
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

@app.post("/api/sessions/{session_id}/events")
def ingest_events(session_id: str, payload: dict, db: DBSession = Depends(get_db)):
    # Find or create the session summary row
    session = db.get(Session, session_id)
    if not session:
        session = Session(id=session_id, page_url=payload.get("page_url"), rage_click_count=0)
        db.add(session)

    # Count any rage-clicks in this batch and update the summary
    events = payload.get("events", [])
    rage_clicks_in_batch = sum(1 for e in events if e.get("type") == "rage_click")
    session.rage_click_count += rage_clicks_in_batch

    # Store the raw event batch
    raw_event = SessionEvent(session_id=session_id, event_blob=events)
    db.add(raw_event)

    db.commit()
    return {"status": "ok", "events_received": len(events)}

@app.get("/api/sessions")
def list_sessions(db: DBSession = Depends(get_db)):
    sessions = db.query(Session).all()
    return sessions