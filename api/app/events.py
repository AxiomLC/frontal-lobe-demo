# ===============  events.py — live pipeline event stream  ===============
# Every stage of an ingest emits here. Two consumers:
#   · GET /api/events  — polling history (newest first, in-memory ring)
#   · GET /api/logs    — Server-Sent Events (each new event is pushed to any
#                        connected subscriber's queue as it happens)
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

_MAX_EVENTS = 500
_events: list[dict] = []
_subscribers: set[asyncio.Queue] = set()


def emit(level: str, message: str, ticker: str = "", job_id: str = "", leg: str = "usaspending") -> None:
    """Append one event and broadcast it to every live SSE subscriber."""
    ev = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,  # info | ok | warn | error
        "message": message,
        "ticker": ticker,
        "job_id": job_id,
        "leg": leg,
    }
    _events.append(ev)
    if len(_events) > _MAX_EVENTS:
        del _events[: len(_events) - _MAX_EVENTS]
    stale = []
    for q in _subscribers:
        try:
            q.put_nowait(ev)
        except asyncio.QueueFull:
            stale.append(q)
        except Exception:  # noqa: BLE001 - a slow/dead subscriber never breaks emit
            stale.append(q)
    for q in stale:
        _subscribers.discard(q)


def list_events(limit: int = 200) -> list[dict]:
    """Newest first."""
    return list(reversed(_events[-limit:]))


def subscribe() -> asyncio.Queue:
    """A subscriber queue that receives every event as it is emitted."""
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _subscribers.add(q)
    return q


def unsubscribe(q: asyncio.Queue) -> None:
    _subscribers.discard(q)
