# ===============  server.py — the Frontal Lobe demo FastAPI app (port 8765)  ===============
# Serves the local control-panel UI at / and a small REST API under /api.
# Connect-first: the user pastes their OWN Neo4j (AuraDB Free or local) details
# once; they are saved to runtime/connection.json (per-user, git-ignored) so
# this demo is independent of the full app's .env database.
# The demo ships ONE evidence leg (USASpending). SEC / USPTO / Marlowe /
# semantic / TTS endpoints are friendly 501 placeholders - present so the API
# surface is honest, but they tell the reader those live in the full system.
from __future__ import annotations

import asyncio
import json
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import db, events
from . import usaspending
from .award_config import (
    AWARD_MIN_AMOUNT,
    AWARD_RECIPE_VERSION,
    MAX_AWARDS_PER_TICKER,
    SUBAWARD_MIN_AMOUNT,
)

ROOT = Path(__file__).resolve().parent.parent          # repo root
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

app = FastAPI(title="Frontal Lobe - demo", version=AWARD_RECIPE_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pipeline events stream to the UI log.
usaspending.set_emitter(events.emit)

# Small in-memory job registry (like the private repo's jobs).
_jobs: dict[str, dict] = {}
_job_seq = 0
_job_lock = threading.Lock()


def _next_job_id(leg: str) -> str:
    global _job_seq
    with _job_lock:
        _job_seq += 1
        jid = f"{leg}-{_job_seq}"
    _jobs[jid] = {"id": jid, "leg": leg, "status": "running", "started_at": time.time()}
    return jid


def _finish_job(jid: str, status: str, extra=None):
    if jid in _jobs:
        _jobs[jid].update(status=status, finished_at=time.time())
        if extra:
            _jobs[jid].update(extra)


# ── request models ─────────────────────────────────────────
class ConnectBody(BaseModel):
    uri: str = ""
    username: str = ""
    password: str = ""


class IngestBody(BaseModel):
    tickers: list[str] = []
    dry_run: bool = False


class CypherBody(BaseModel):
    query: str = ""
    types: list[str] = []
    hidden_bands: list[str] = []
    limit: int = 200


class SemanticBody(BaseModel):
    text: str = ""
    types: list[str] = []
    hidden_bands: list[str] = []
    limit: int = 20

# ── driver/session helpers ──────────────────────────────────
def _get_driver():
    """Return the cached driver or raise a friendly 503 (not connected yet)."""
    if not db.has_connection():
        raise HTTPException(status_code=503, detail="Not connected - open the app and Save a Neo4j connection first.")
    try:
        return db.get_driver()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neo4j connection not ready: {e}")


def _run_read(fn):
    """Run a read-only function inside a Neo4j session via the thread pool."""
    driver = _get_driver()

    def work():
        with driver.session() as session:
            return fn(session)

    try:
        return work()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"neo4j unreachable: {e}")


# ── ops: status / connect / health / config ─────────────────
@app.get("/api/status")
def status():
    return {
        "connected": db.has_connection(),
        "saved": bool((Path(ROOT) / "runtime" / "connection.json").exists()),
        "uri": db.masked_uri(),
        "recipe": AWARD_RECIPE_VERSION,
    }


@app.post("/api/connect/test")
def connect_test(body: ConnectBody):
    ok, msg = db.test_connection(body.uri.strip(), body.username.strip(), body.password)
    return {"ok": ok, "message": msg}


@app.post("/api/connect/save")
def connect_save(body: ConnectBody):
    uri, username = body.uri.strip(), body.username.strip()
    if not uri or not username:
        raise HTTPException(status_code=400, detail="URI and username are required.")
    if not password_provided(body.password):
        # keep the existing saved password unless a new one is given
        existing = db.load_connection()
        password = body.password or existing.get("password", "")
    else:
        password = body.password
    db.save_connection(uri, username, password)
    return {"ok": True, "uri": db.masked_uri()}


def password_provided(pw: str) -> bool:
    return bool(pw)


@app.get("/api/health")
def health():
    connected = db.has_connection()
    neo4j = False
    if connected:
        try:
            _run_read(lambda s: s.run("RETURN 1").single())
            neo4j = True
        except HTTPException:
            neo4j = False
    return {"status": "ok", "connected": connected, "neo4j": neo4j}


@app.get("/api/config")
def config():
    """The UI tuning knobs - single source of truth = award_config.py."""
    return {
        "award_recipe_version": AWARD_RECIPE_VERSION,
        "max_awards_per_ticker": MAX_AWARDS_PER_TICKER,
        "award_min_amount": AWARD_MIN_AMOUNT,
        "subaward_min_amount": SUBAWARD_MIN_AMOUNT,
    }


@app.get("/api/examples")
def examples():
    """The starter Cypher queries for the UI's example panel."""
    return [
        {"title": "Top federal awards for a company",
         "cypher": "MATCH (t:TICKER {ticker: 'IONQ'})-[:HAS_AWARD]->(a:FEDERAL_AWARD)\n"
                   "RETURN a.name, a.role, a.kind, a.amount, a.agency_toptier\n"
                   "ORDER BY a.amount DESC"},
        {"title": "What products do the awards cover?",
         "cypher": "MATCH (t:TICKER)-[:HAS_AWARD]->(a:FEDERAL_AWARD)-[:COVERS]->(p:PRODUCT)\n"
                   "RETURN t.ticker, p.name, a.amount ORDER BY a.amount DESC LIMIT 25"},
        {"title": "Prime contractors a company works under",
         "cypher": "MATCH (t:TICKER {ticker: 'INFQ'})-[:HAS_AWARD]->(a:FEDERAL_AWARD {role: 'subcontract'})\n"
                   "-[:AWARDED_BY]->(prime:COMPANY)\n"
                   "RETURN prime.name, count(a) AS awards, sum(a.amount) AS total\nORDER BY total DESC"},
        {"title": "Whole graph, without the internal chunks",
         "cypher": "MATCH (n) WHERE NOT n:Chunk AND NOT n:_Heartbeat\n"
                   "OPTIONAL MATCH (n)-[r]->(m) WHERE NOT m:Chunk AND NOT m:_Heartbeat\n"
                   "RETURN n, r, m"},
    ]

# ── ingest (background) + jobs + events + logs ──────────────
@app.post("/api/ingest")
def ingest(body: IngestBody):
    """Run the USASpending leg in a background task. dry_run previews the plan
    without writing anything."""
    tickers = [t.upper() for t in body.tickers if (t or "").strip()]
    jid = _next_job_id("usaspending")
    events.emit("info", f"USASpending ingest job {jid} accepted", job_id=jid)

    def work():
        result = usaspending.run_awards(tickers, dry_run=body.dry_run, job_id=jid)
        status = "error" if result.get("status") == "error" else "success"
        _finish_job(jid, status, {"result": result})

    threading.Thread(target=work, daemon=True).start()
    return {"status": "accepted", "job_id": jid, "dry_run": body.dry_run}


@app.post("/api/ingest-usaspending")
def ingest_usaspending_alias(body: IngestBody):
    """UI-compatible alias of /api/ingest (the UI's USASpending button)."""
    return ingest(body)


@app.get("/api/jobs")
def jobs_list():
    out = [dict(j) for j in _jobs.values()]
    out.sort(key=lambda j: j.get("started_at", 0), reverse=True)
    return out


@app.get("/api/events")
def events_list(limit: int = 200):
    return events.list_events(limit=limit)


@app.get("/api/logs")
async def logs_stream(request: Request):
    """Server-Sent Events stream of ingest progress."""
    queue = events.subscribe()

    async def stream():
        try:
            # send a hello so the client knows the stream is live
            yield "data: {\"level\":\"info\",\"message\":\"connected\"}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    ev = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {json.dumps(ev)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            events.unsubscribe(queue)

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.post("/api/keepalive")
def keepalive():
    """Perform ONE tiny WRITE so a Free instance's 72h (write-based) inactivity
    clock resets - reads do NOT count on AuraDB Free. Creates exactly one
    _Heartbeat node, never more."""
    def work(session):
        session.run("MERGE (h:_Heartbeat {id:'keepalive'}) SET h.ts = timestamp()").consume()
        return True
    _run_read(work)
    return {"ok": True, "note": "wrote _Heartbeat keepalive"}


@app.get("/api/counts")
def counts():
    """Node + relationship counts per label (heartbeat excluded) + usage
    totals against the AuraDB Free 200k/400k limits."""
    from .writer import per_label_counts as _plc

    data = _run_read(lambda s: _plc(s))
    data["node_limit"] = 200_000
    data["rel_limit"] = 400_000
    data["node_pct"] = round(100.0 * data["nodes_total"] / data["node_limit"], 2)
    data["rel_pct"] = round(100.0 * data["rels_total"] / data["rel_limit"], 2)
    return data

# ── UI read endpoints ───────────────────────────────────────
@app.get("/api/stats")
def stats():
    def work(s):
        nodes = s.run("MATCH (n:__Entity__) RETURN count(n) AS c").single()["c"]
        chunks = s.run("MATCH (n:Chunk) RETURN count(n) AS c").single()["c"]
        edges = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        return {"nodes": int(nodes), "chunks": int(chunks), "edges": int(edges)}
    return _run_read(work)


@app.get("/api/kb")
def kb():
    """Knowledge-base summary - USASpending only."""
    def work(s):
        totals = {}
        for r in s.run(
            "MATCH (n:__Entity__) WHERE NOT n:_Heartbeat "
            "RETURN labels(n) AS l"
        ):
            for lab in r["l"]:
                if lab in ("__Entity__", "__Node__"):
                    continue
                totals[lab] = totals.get(lab, 0) + 1
        per = {}
        for r in s.run(
            "MATCH (t:TICKER) WHERE NOT t:_Heartbeat "
            "OPTIONAL MATCH (t)-[:HAS_AWARD]->(a:FEDERAL_AWARD) "
            "OPTIONAL MATCH (t)-[:HAS_AWARD_INVENTORY]->(inv:AWARD_INVENTORY) "
            "OPTIONAL MATCH (t)-[:HAS_AWARD]->(:FEDERAL_AWARD)-[:COVERS]->(p:PRODUCT) "
            "RETURN t.ticker AS ticker, count(DISTINCT a) AS awards, "
            "count(DISTINCT p) AS products, inv.award_count AS inventory_count, "
            "coalesce(max(a.amount),0) AS top_amount"
        ):
            per[r["ticker"]] = {
                "awards": int(r["awards"] or 0),
                "products": int(r["products"] or 0),
                "inventory_count": int(r["inventory_count"] or 0),
                "top_amount": r["top_amount"] or 0,
            }
        return {"totals": totals, "per_ticker": per}
    return _run_read(work)


@app.get("/api/tickers")
def tickers():
    """Per-ticker ingest records (re-run warning in the UI)."""
    def work(s):
        rows = []
        for r in s.run(
            "MATCH (t:TICKER) WHERE NOT t:_Heartbeat "
            "RETURN t.ticker AS ticker, t.award_recipe_version AS ver, "
            "t.company_name AS company"
        ):
            rows.append({"ticker": r["ticker"], "extraction_version": r["ver"], "company_name": r["company"] or r["ticker"]})
        return rows
    return _run_read(work)


@app.get("/api/graph")
def graph():
    """Nodes + links for the 2D map - entities only, no chunks/heartbeat."""
    from .writer import graph_subgraph
    return _run_read(lambda s: graph_subgraph(s))


@app.get("/api/node/{node_id}")
def node_detail(node_id: str):
    """Viewer payload: summary + the full source chunk + 1-hop neighbors."""
    from urllib.parse import quote

    def work(s):
        nrow = s.run(
            "MATCH (n {id:$id}) RETURN n.name AS name, labels(n) AS labels, "
            "n.summary AS summary, properties(n) AS props",
            id=node_id,
        ).single()
        if not nrow:
            raise HTTPException(status_code=404, detail="Node not found")
        labs = [l for l in (nrow["labels"] or []) if l not in ("__Entity__", "__Node__")]
        # the chunk text(s) that MENTIONS this node (usually one big chunk)
        chunks = []
        for r in s.run(
            "MATCH (c:Chunk)-[:MENTIONS]->(n {id:$id}) RETURN c.text AS text ORDER BY coalesce(c.chunk_index,0)",
            id=node_id,
        ):
            if r["text"]:
                chunks.append(r["text"])
        # 1-hop neighbors for the connections list
        neighbors = []
        for r in s.run(
            "MATCH (n {id:$id})-[rel]->(m) WHERE NOT m:Chunk AND NOT m:_Heartbeat "
            "RETURN m.id AS id, m.name AS name, labels(m) AS ml, type(rel) AS rel",
            id=node_id,
        ):
            ml = [l for l in (r["ml"] or []) if l not in ("__Entity__", "__Node__")]
            neighbors.append({"id": r["id"], "name": r["name"], "type": ml[0] if ml else "NODE", "rel": r["rel"]})
        for r in s.run(
            "MATCH (m)-[rel]->(n {id:$id}) WHERE NOT m:Chunk AND NOT m:_Heartbeat "
            "RETURN m.id AS id, m.name AS name, labels(m) AS ml, type(rel) AS rel",
            id=node_id,
        ):
            ml = [l for l in (r["ml"] or []) if l not in ("__Entity__", "__Node__")]
            neighbors.append({"id": r["id"], "name": r["name"], "type": ml[0] if ml else "NODE", "rel": r["rel"]})
        return {
            "id": node_id,
            "name": nrow["name"],
            "type": labs[0] if labs else "NODE",
            "props": nrow["props"] or {},
            "summary": nrow["summary"] or "",
            "chunks": chunks,
            "neighbors": neighbors,
        }
    return _run_read(work)


_WRITE_HINTS = (
    "create ", " delete ", " set ", " merge ", " remove ", " detach ",
    "drop ", " call db.", " load csv", "foreach (", "unwind ",
)


@app.post("/api/cypher")
def cypher(body: CypherBody):
    """Read-only Cypher, Utilities-scoped. Writes are blocked; LIMIT capped."""
    q = (body.query or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="Empty query")
    low = " " + " ".join(q.lower().split()) + " "
    if any(h in low for h in _WRITE_HINTS):
        raise HTTPException(status_code=400, detail="Read-only mode - writes are not allowed here.")
    lim = max(1, min(int(body.limit or 200), 500))

    def work(s):
        s = s.run(q, limit=lim)
        keys = list(s.keys())
        rows = [dict(r) for r in s]
        return {"keys": keys, "rows": rows, "meta": {"count": len(rows), "limit": lim}}
    return _run_read(work)


@app.post("/api/semantic")
def semantic(body: SemanticBody):
    """Semantic (vector) search is part of the full system - not this demo."""
    raise HTTPException(
        status_code=501,
        detail="Semantic search is part of the full Frontal Lobe system - this demo ships the deterministic USASpending leg only.",
    )

# ── placeholders: SEC / USPTO / Marlowe / semantic / TTS ───
_FULL_SYSTEM = "This is part of the full Frontal Lobe system (SEC contracts, USPTO patents, Marlowe, etc.) - not included in this USASpending demo slice."

SEC_STUB = {
    "SEC leg": "Finds material contracts (EX-2/EX-10), announcements, subsidiary lists, and extracts clauses as CONTRACT_TERM hubs. In the full system.",
    "status": "not-in-demo",
}


@app.post("/api/ingest-sec")
def ingest_sec():
    raise HTTPException(status_code=501, detail=_FULL_SYSTEM)


@app.post("/api/ingest-patents")
def ingest_patents():
    raise HTTPException(status_code=501, detail=_FULL_SYSTEM)


@app.post("/api/marlowe/chat")
async def marlowe_chat():
    raise HTTPException(status_code=501, detail=_FULL_SYSTEM)


@app.post("/api/marlowe/reset")
def marlowe_reset():
    raise HTTPException(status_code=501, detail=_FULL_SYSTEM)


@app.post("/api/marlowe/stop")
def marlowe_stop():
    raise HTTPException(status_code=501, detail=_FULL_SYSTEM)


@app.post("/api/marlowe/mint")
def marlowe_mint():
    raise HTTPException(status_code=501, detail=_FULL_SYSTEM)


@app.get("/api/marlowe/trace")
def marlowe_trace():
    return {"runs": []}


@app.post("/api/tts")
def tts():
    raise HTTPException(status_code=501, detail=_FULL_SYSTEM)


# ── misc ops ────────────────────────────────────────────────
@app.post("/api/delete-ticker")
def delete_ticker(body: IngestBody):
    """Remove one ticker's owned nodes (shared entities survive)."""
    def work(s):
        for t in (body.tickers or []):
            s.run(
                "MATCH (n) WHERE n.ticker = $t AND "
                "(n:TICKER OR n:FEDERAL_AWARD OR n:AWARD_INVENTORY OR n:PRODUCT "
                " OR (n:COMPANY AND n.source='USASpending') OR n:SUBSIDIARY) "
                "DETACH DELETE n",
                t=str(t).upper(),
            ).consume()
        return {"ok": True}
    return _run_read(work)


@app.post("/api/rebuild")
def rebuild():
    # No cross-leg canonical merge in the demo (single leg) - no-op by design.
    events.emit("ok", "Rebuild: no-op (single-leg demo has no cross-leg merge to run)")
    return {"ok": True, "note": "Single-leg demo - no cross-leg canonical merge."}


@app.post("/api/truncate")
def truncate():
    from .writer import clear_all
    events.emit("warn", "Truncate: wiping the entire graph")
    return _run_read(clear_all)


@app.get("/api/export")
def export_graph(lean: bool = False):
    """Full graph backup JSON (Free Aura has no backup) - restore-ready."""
    def work(s):
        nodes, rels = [], []
        for r in s.run("MATCH (n) WHERE NOT n:_Heartbeat RETURN n"):
            d = dict(r["n"])
            if lean:
                d.pop("embedding", None)
            nodes.append({"id": d.get("id"), "labels": r["n"].labels, "props": d})
        for r in s.run(
            "MATCH (a)-[rel]->(b) "
            "WHERE NOT a:_Heartbeat AND NOT b:_Heartbeat "
            "RETURN startNode(rel).id AS s, endNode(rel).id AS t, type(rel) AS type, properties(rel) AS props"
        ):
            rels.append({"source": r["s"], "target": r["t"], "type": r["type"], "props": r["props"]})
        return {"counts": {"nodes": len(nodes), "relationships": len(rels)}, "nodes": nodes, "relationships": rels}
    return _run_read(work)


@app.post("/api/restore")
def restore_graph(payload: dict, dry_run: bool = False):
    """Restore from a /api/export JSON - wipes the graph first, then reimports
    nodes (per-label) and relationships (per-type) with plain Cypher (no APOC)."""
    from .writer import _entity_upsert_with_label, _rel_upsert_with_label

    nodes = payload.get("nodes") or []
    rels = payload.get("relationships") or []

    def work(s):
        tx = s.begin_transaction()
        try:
            tx.run("MATCH (n) DETACH DELETE n")
            by_label = {}
            for n in nodes:
                lab = "NODE"
                lbl = [l for l in (n.get("labels") or []) if l not in ("__Node__", "__Entity__")]
                if lbl:
                    lab = lbl[0]
                by_label.setdefault(lab, []).append({"id": n.get("id"), "name": (n.get("props") or {}).get("name"), "props": n.get("props") or {}})
            for lab, rows in by_label.items():
                tx.run(_entity_upsert_with_label(lab), data=rows)
            by_type = {}
            for rel in rels:
                by_type.setdefault(rel.get("type", "REL"), []).append(
                    {"source_id": rel.get("source"), "target_id": rel.get("target"), "props": rel.get("props") or {}}
                )
            for typ, rows in by_type.items():
                tx.run(_rel_upsert_with_label(typ), data=rows)
            if dry_run:
                tx.rollback()
                return {"dry_run": True, "nodes_imported": len(nodes)}
            tx.commit()
            return {"nodes_imported": len(nodes), "relationships_imported": len(rels)}
        except Exception:
            tx.rollback()
            raise
    return _run_read(work)


# ── static UI ───────────────────────────────────────────────
@app.get("/")
def index():
    f = STATIC_DIR / "index.html"
    if not f.exists():
        return JSONResponse({"name": "Frontal Lobe demo", "docs": "see TECH_README.md"}, status_code=200)
    return FileResponse(f)


# ── runtime ─────────────────────────────────────────────────
def main():
    import uvicorn
    uvicorn.run("api.app.server:app", host="127.0.0.1", port=8765, reload=False)


if __name__ == "__main__":
    main()
