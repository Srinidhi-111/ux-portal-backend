from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session as DBSession
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from database import engine, get_db
from models import Base, Session, SessionEvent
import datetime
from fastapi import WebSocket, WebSocketDisconnect

connected_clients: list[WebSocket] = []
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
def list_sessions(
    search: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    db: DBSession = Depends(get_db),
):
    query = db.query(Session)

    if search:
        query = query.filter(Session.page_url.ilike(f"%{search}%"))

    if start_date:
        start_dt = datetime.datetime.fromisoformat(start_date)
        query = query.filter(Session.start_time >= start_dt)

    if end_date:
        end_dt = datetime.datetime.fromisoformat(end_date)
        query = query.filter(Session.start_time <= end_dt)

    sessions = query.order_by(Session.start_time.desc()).all()

    result = []
    for s in sessions:
        duration_ms = 0
        if s.end_time:
            duration_ms = int((s.end_time - s.start_time).total_seconds() * 1000)

        if s.rage_click_count >= 3:
            status = "RAGE_CLICK"
        elif duration_ms > 0 and duration_ms < 10000:
            status = "ABANDONED"
        else:
            status = "NORMAL"

        result.append({
            "id": s.id,
            "page_url": s.page_url,
            "start_time": s.start_time.isoformat(),
            "end_time": s.end_time.isoformat() if s.end_time else None,
            "rage_click_count": s.rage_click_count,
            "device_type": s.device_type,
            "duration_ms": duration_ms,
            "status": status,
        })
    return result

@app.post("/api/sessions/{session_id}/events")
async def ingest_events(session_id: str, payload: dict, db: DBSession = Depends(get_db)):
    session = db.get(Session, session_id)
    if not session:
        session = Session(id=session_id, page_url=payload.get("page_url"), rage_click_count=0)
        db.add(session)
        try:
            db.flush()
        except IntegrityError:
            db.rollback()
            session = db.get(Session, session_id)

    events = payload.get("events", [])
    rage_clicks_in_batch = sum(
        1 for e in events
        if e.get("source") == "custom" and e.get("type") == "rage_click"
    )
    session.rage_click_count += rage_clicks_in_batch
    session.end_time = datetime.datetime.utcnow()

    raw_event = SessionEvent(session_id=session_id, event_blob=events)
    db.add(raw_event)

    db.commit()

    # Notify any connected dashboards that this session was updated
    import json
    for client in connected_clients:
        try:
            await client.send_text(json.dumps({
                "session_id": session_id,
                "page_url": session.page_url,
                "rage_click_count": session.rage_click_count,
            }))
        except Exception:
            pass

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

@app.get("/api/sessions/{session_id}/raw")
def get_raw_events(session_id: str, db: DBSession = Depends(get_db)):
    raw_batches = (
        db.query(SessionEvent)
        .filter(SessionEvent.session_id == session_id)
        .order_by(SessionEvent.id)
        .all()
    )

    all_events = []
    for batch in raw_batches:
        all_events.extend(batch.event_blob)

    return all_events

@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)

@app.get("/api/sessions/stats")
def get_stats(db: DBSession = Depends(get_db)):
    sessions = db.query(Session).all()
    total = len(sessions)

    if total == 0:
        return {"total_sessions": 0, "bounce_rate": 0, "avg_duration_ms": 0, "total_rage_clicks": 0}

    durations = []
    bounced = 0
    total_rage = 0

    for s in sessions:
        total_rage += s.rage_click_count
        if s.end_time:
            duration_ms = int((s.end_time - s.start_time).total_seconds() * 1000)
            durations.append(duration_ms)
            if duration_ms < 10000:
                bounced += 1

    avg_duration = int(sum(durations) / len(durations)) if durations else 0
    bounce_rate = round((bounced / total) * 100, 1)

    return {
        "total_sessions": total,
        "bounce_rate": bounce_rate,
        "avg_duration_ms": avg_duration,
        "total_rage_clicks": total_rage,
    }

@app.get("/api/heatmap")
def get_heatmap(session_id: str | None = None, db: DBSession = Depends(get_db)):
    query = db.query(SessionEvent)
    if session_id:
        query = query.filter(SessionEvent.session_id == session_id)
    all_events = query.all()

    zones: dict[int, int] = {}
    points = []

    for batch in all_events:
        for item in batch.event_blob:
            if item.get("source") == "custom" and "y" in item and "x" in item:
                zone_start = (item["y"] // 200) * 200
                zones[zone_start] = zones.get(zone_start, 0) + 1
                points.append({"x": item["x"], "y": item["y"]})

    zone_list = [
        {"range": f"{start}-{start+200}px", "clicks": count}
        for start, count in zones.items()
    ]
    zone_list.sort(key=lambda z: z["clicks"], reverse=True)

    return {"zones": zone_list, "points": points[-300:]}

@app.get("/api/rage-clicks")
def get_rage_clicks(session_id: str | None = None, db: DBSession = Depends(get_db)):
    query = db.query(SessionEvent).order_by(SessionEvent.id.desc())
    if session_id:
        query = query.filter(SessionEvent.session_id == session_id)
    all_events = query.limit(100).all()

    alerts = []
    for batch in all_events:
        for item in batch.event_blob:
            if item.get("source") == "custom" and item.get("type") == "rage_click":
                alerts.append({
                    "element_id": item.get("elementId", "unknown"),
                    "severity": item.get("severity", "medium").upper(),
                    "timestamp": item.get("timestamp"),
                })

    alerts.sort(key=lambda a: a["timestamp"] or 0, reverse=True)
    return alerts[:20]

@app.get("/api/top-pages")
def get_top_pages(db: DBSession = Depends(get_db)):
    sessions = db.query(Session).all()
    totals: dict[str, int] = {}
    for s in sessions:
        totals[s.page_url] = totals.get(s.page_url, 0) + s.rage_click_count

    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    return [{"page_url": url, "rage_click_count": count} for url, count in ranked[:5] if count > 0]

