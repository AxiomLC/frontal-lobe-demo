# ===============  mcp_server.py — MCP exposure of the demo (award leg)  ===============
# Lets an external agent drive the demo over Model Context Protocol: ingest the
# USASpending leg, poll jobs, and run read-only queries (cypher, kb, graph,
# node detail). Deliberately a SUBSET of the private frontal-lobe MCP host -
# no SEC/USPTO/Marlowe/semantic here (those endpoints tell the agent they live
# in the full system).
#
# Auth (HTTP only, same pattern as the private repo): static bearer
#   `Authorization: Bearer <MCP_API_KEY>`  OR  basic `Authorization: Basic
#   base64(any:key)` (the key is the password half; username ignored).
#   No key configured -> open (dev). Over stdio the transport is the gate.
#
# Run standalone (stdio):  python -m api.app.mcp_server
# The FastAPI app mounts this at /mcp (see server.py) with the auth gate.
import argparse
import asyncio
import base64
import os
import threading

from fastmcp import FastMCP

from . import events as eventlog
from . import db
from . import usaspending
from . import writer

mcp = FastMCP("frontal-lobe-demo")


def _clip(s, n=120):
    s = ((s or "").strip()).replace("\n", " ")
    return s if len(s) <= n else s[: n - 1] + "..."


# ── AUTH (HTTP only) ────────────────────────────────────────
def _check_auth(header: str) -> bool:
    """Bearer <MCP_API_KEY> or Basic (any user):<MCP_API_KEY>. No key -> open."""
    key = os.getenv("MCP_API_KEY", "").strip()
    if not key:
        return True
    h = (header or "").strip()
    low = h.lower()
    if low.startswith("bearer "):
        return h[7:].strip() == key
    if low.startswith("basic "):
        try:
            raw = base64.b64decode(h[6:].strip()).decode()
            _, _, p = raw.partition(":")
            return p == key
        except Exception:  # noqa: BLE001 - bad base64 is a failed login
            return False
    return False


class _AuthMiddleware:
    """Pure-ASGI gate: 401 unless the Authorization header passes."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers") or [])
            if not _check_auth(headers.get(b"authorization", b"").decode()):
                body = b'{"detail":"unauthorized"}'
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [(b"content-type", b"application/json")],
                })
                await send({"type": "http.response.body", "body": body})
                return
        await self.app(scope, receive, send)



def protected_app():
    """The FastMCP HTTP ASGI app wrapped in the auth gate - for an optional
    standalone HTTP runner (bash / python -m). Returns (app, lifespan)."""
    app = mcp.http_app(path="/mcp")
    return _AuthMiddleware(app)


def main():
    parser = argparse.ArgumentParser(description="Frontal Lobe demo MCP server")
    parser.add_argument("--transport", choices=["stdio", "http"], default=os.getenv("MCP_TRANSPORT", "stdio"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MCP_PORT", "9000")))
    args = parser.parse_args()
    if args.transport == "http":
        import uvicorn

        uvicorn.run(protected_app(), host="127.0.0.1", port=args.port, log_level="warning")
    else:
        # stdio - the transport itself is the gate (no auth needed).
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
