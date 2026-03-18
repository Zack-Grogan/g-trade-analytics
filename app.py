"""
g-trade-analytics: G-Trade analytics service; read-only API over Postgres for runs, events, trades.
Auth: Bearer ANALYTICS_API_KEY (single-operator).
"""
from __future__ import annotations

import os
import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Query, status
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ANALYTICS_API_KEY = os.environ.get("ANALYTICS_API_KEY", "")

_pool: ThreadedConnectionPool | None = None


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL not set")
        _pool = ThreadedConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL)
        logger.info("Analytics Postgres pool initialised (minconn=1, maxconn=5)")
    return _pool


def get_conn():
    return _get_pool().getconn()


def put_conn(conn) -> None:
    try:
        _get_pool().putconn(conn)
    except Exception:
        logger.warning("put_conn: failed to return connection to pool", exc_info=True)


def ensure_schema_v2() -> None:
    """Apply RLM/analytics extension schema (hypotheses, experiments, knowledge_store, trade_embeddings, memory graph)."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema_v2.sql")
    if not os.path.exists(schema_path):
        return
    conn = get_conn()
    try:
        with open(schema_path) as f:
            conn.cursor().execute(f.read())
        conn.commit()
    finally:
        put_conn(conn)


def _bearer_ok(authorization: str | None) -> bool:
    if not ANALYTICS_API_KEY:
        return False
    if not authorization or not authorization.startswith("Bearer "):
        return False
    return authorization[7:].strip() == ANALYTICS_API_KEY.strip()


def _require_auth(authorization: str | None) -> None:
    if not _bearer_ok(authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing Bearer token")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        ensure_schema_v2()
    except Exception as e:
        logger.warning("Startup schema_v2 ensure failed: %s", e)
    yield


app = FastAPI(title="g-trade-analytics", lifespan=lifespan)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


@app.get("/health")
async def health():
    return {"status": "ok"}


# GraphQL (advisory-only mutations; Bearer auth via info.context["request"])
from strawberry.fastapi import GraphQLRouter
from graphql_schema import schema

graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8001")))
