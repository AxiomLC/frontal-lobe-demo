# ===============  db.py — Neo4j driver for the demo app  ===============
# The demo connects to YOUR Neo4j (AuraDB Free or a local install). Details
# are saved locally in runtime/connection.json (git-ignored) — the SAME shape the
# full Frontal Lobe app keeps in its .env, but stored per-user here so this
# demo can point at its OWN database without touching anything else.
# The password is never logged and is masked in every API response.
import json
import threading
from pathlib import Path

from neo4j import GraphDatabase

_DB_FILE = Path(__file__).resolve().parent.parent.parent / "runtime" / "connection.json"

_driver = None
_driver_lock = threading.Lock()
_conn = {"uri": "", "username": "", "password": ""}


def _data_dir() -> Path:
    return _DB_FILE.parent


def load_connection() -> dict:
    """The saved (uri, username, password). Password never echoes to callers
    directly — use masked_uri() / has_connection() for UI-facing values."""
    global _conn
    try:
        if _DB_FILE.exists():
            _conn = json.loads(_DB_FILE.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - corrupt file -> treat as not connected
        _conn = {"uri": "", "username": "", "password": ""}
    return _conn


def _reset_driver() -> None:
    global _driver
    with _driver_lock:
        if _driver is not None:
            try:
                _driver.close()
            except Exception:  # noqa: BLE001
                pass
            _driver = None


def has_connection() -> bool:
    load_connection()
    return bool(_conn.get("uri") and _conn.get("username"))


def masked_uri() -> str:
    """e.g. neo4j+s://xxxx....databases.neo4j.io (host only, scheme kept)."""
    load_connection()
    uri = _conn.get("uri") or ""
    if not uri:
        return ""
    # strip credentials if any, keep scheme + host-ish tail
    host = uri.split("@")[-1]
    return host


def get_driver():
    """Lazily-built driver singleton from the saved connection. Raises a clear
    error when not connected yet."""
    global _driver
    load_connection()
    if not (_conn.get("uri") and _conn.get("username") and _conn.get("password")):
        raise RuntimeError("Not connected — open the app and Save a Neo4j connection first.")
    with _driver_lock:
        if _driver is None:
            _driver = GraphDatabase.driver(
                _conn["uri"],
                auth=(_conn["username"], _conn["password"]),
                connection_timeout=15,
            )
    return _driver


def save_connection(uri: str, username: str, password: str) -> None:
    """Persist the connection locally and rebuild the driver. Never logs the
    password — the only place it is written is runtime/connection.json."""
    _data_dir().mkdir(parents=True, exist_ok=True)
    _DB_FILE.write_text(
        json.dumps({"uri": uri, "username": username, "password": password}, indent=2),
        encoding="utf-8",
    )
    load_connection()
    _reset_driver()


def test_connection(uri: str, username: str, password: str) -> tuple[bool, str]:
    """Open a throwaway driver and run RETURN 1. Returns (ok, human message)
    distinguishing bad credentials / paused instance / unreachable / bad URI."""
    if not (uri or "").strip():
        return False, "Connection URI is required (looks like neo4j+s://xxx.databases.neo4j.io)."
    low = uri.lower()
    if not any(low.startswith(p) for p in ("neo4j+s://", "neo4j://", "bolt+s://", "bolt://")):
        return False, "Bad URI format — use neo4j+s:// (Aura), or neo4j:// / bolt:// for a local install."
    driver = None
    try:
        driver = GraphDatabase.driver(uri, auth=(username or "", password or ""), connection_timeout=15)
        with driver.session() as s:
            s.run("RETURN 1").single()
        return True, "Connected — the database is reachable."
    except Exception as e:  # noqa: BLE001 - map to a friendly message
        msg = str(e).lower()
        if "unauthorized" in msg or "authentication" in msg or "auth" in msg:
            return False, "Wrong username or password — check the credentials file."
        if "paused" in msg or "database is unavailable" in msg or "connection is closed" in msg:
            return False, "The instance looks paused — open the Neo4j console and press the play button."
        if "name or service not known" in msg or "failed to establish" in msg or "timed out" in msg:
            return False, "Unreachable — check the URI and that your network allows it."
        if "not a valid" in msg or "uri" in msg or "scheme" in msg:
            return False, "Bad URI format — use the neo4j+s:// URI from your credentials file."
        return False, f"Could not connect: {e}"
    finally:
        if driver is not None:
            try:
                driver.close()
            except Exception:  # noqa: BLE001
                pass
