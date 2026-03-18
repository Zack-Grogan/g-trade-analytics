"""
g-trade-analytics: G-Trade analytics service; read-only API over Postgres for runs, events, trades.
Auth: Bearer ANALYTICS_API_KEY (single-operator).
"""
from __future__ import annotations

import os
import logging
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Query, status
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ANALYTICS_API_KEY = os.environ.get("ANALYTICS_API_KEY", "")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL)


def _bearer_ok(authorization: str | None) -> bool:
    if not ANALYTICS_API_KEY:
        return False
    if not authorization or not authorization.startswith("Bearer "):
        return False
    return authorization[7:].strip() == ANALYTICS_API_KEY.strip()


def _require_auth(authorization: str | None) -> None:
    if not _bearer_ok(authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing Bearer token")


app = FastAPI(title="g-trade-analytics")


@app.get("/runs")
async def list_runs(
    authorization: str | None = Header(None),
    limit: int = Query(25, ge=1, le=100),
    search: Optional[str] = None,
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if search:
            cur.execute(
                """SELECT run_id, created_at, process_id, data_mode, symbol, payload_json
                   FROM runs WHERE run_id ILIKE %s OR data_mode ILIKE %s OR payload_json::text ILIKE %s
                   ORDER BY created_at DESC LIMIT %s""",
                (f"%{search}%", f"%{search}%", f"%{search}%", limit),
            )
        else:
            cur.execute(
                "SELECT run_id, created_at, process_id, data_mode, symbol, payload_json FROM runs ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
        rows = cur.fetchall()
        return {"runs": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/runs/{run_id}")
async def get_run(
    run_id: str,
    authorization: str | None = Header(None),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT run_id, created_at, process_id, data_mode, symbol, payload_json FROM runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        return dict(row)
    finally:
        conn.close()


@app.get("/runs/{run_id}/events")
async def run_events(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(100, ge=1, le=500),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, run_id, event_timestamp, inserted_at, category, event_type, source, symbol, zone, action, reason, order_id, risk_state, payload_json
               FROM events WHERE run_id = %s ORDER BY event_timestamp DESC LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {"events": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/runs/{run_id}/trades")
async def run_trades(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(100, ge=1, le=200),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, run_id, inserted_at, entry_time, exit_time, direction, contracts, entry_price, exit_price, pnl, zone, strategy, regime, source, backfilled, payload_json
               FROM completed_trades WHERE run_id = %s ORDER BY exit_time DESC LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {"trades": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/analytics/summary")
async def analytics_summary(
    authorization: str | None = Header(None),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) AS run_count FROM runs")
        runs = cur.fetchone()["run_count"]
        cur.execute("SELECT COUNT(*) AS event_count FROM events")
        events = cur.fetchone()["event_count"]
        cur.execute("SELECT COUNT(*) AS trade_count, COALESCE(SUM(pnl), 0) AS total_pnl FROM completed_trades")
        row = cur.fetchone()
        return {"run_count": runs, "event_count": events, "trade_count": row["trade_count"], "total_pnl": float(row["total_pnl"] or 0)}
    finally:
        conn.close()


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8001")))
