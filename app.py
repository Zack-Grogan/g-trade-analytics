"""
g-trade-analytics: G-Trade analytics service; read-only API over Postgres for runs, events, trades.
Auth: Bearer ANALYTICS_API_KEY (single-operator).
"""
from __future__ import annotations

import os
import logging
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Optional, Generator

from fastapi import FastAPI, Header, HTTPException, Query, status
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, RealDictConnection

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ANALYTICS_API_KEY = os.environ.get("ANALYTICS_API_KEY", "")

_pool: ThreadedConnectionPool | None = None


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL not set")
        _pool = ThreadedConnectionPool(minconn=2, maxconn=10, dsn=DATABASE_URL)
        logger.info("Analytics Postgres pool initialised (minconn=2, maxconn=10)")
    return _pool


def get_conn():
    return _get_pool().getconn()


def put_conn(conn) -> None:
    try:
        _get_pool().putconn(conn)
    except Exception:
        logger.warning("put_conn: failed to return connection to pool", exc_info=True)


@contextmanager
def get_db_cursor() -> Generator[Any, None, None]:
    """Context manager for database connections with proper cleanup.
    
    Ensures cursor and connection are properly returned to pool even on error.
    """
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        yield cur
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                logger.warning("Failed to close cursor", exc_info=True)
        if conn is not None:
            put_conn(conn)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def check_pool_health() -> dict[str, Any]:
    """Check connection pool health and return stats."""
    pool = _get_pool()
    return {
        "pool_size": max(0, pool._used - pool._pool.qsize()) if hasattr(pool, '_pool') else "unknown",
        "pool_available": pool._pool.qsize() if hasattr(pool, '_pool') else "unknown",
    }


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
                """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol, status, zone, zone_state,
                          position, position_pnl, daily_pnl, risk_state, last_entry_block_reason, payload_json
                   FROM runs
                   WHERE run_id ILIKE %s
                      OR data_mode ILIKE %s
                      OR status ILIKE %s
                      OR zone ILIKE %s
                      OR risk_state ILIKE %s
                      OR payload_json::text ILIKE %s
                   ORDER BY COALESCE(last_seen_at, created_at) DESC
                   LIMIT %s""",
                (f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", limit),
            )
        else:
            cur.execute(
                """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol, status, zone, zone_state,
                          position, position_pnl, daily_pnl, risk_state, last_entry_block_reason, payload_json
                   FROM runs
                   ORDER BY COALESCE(last_seen_at, created_at) DESC
                   LIMIT %s""",
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
        cur.execute(
            """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol, status, zone, zone_state,
                      position, position_pnl, daily_pnl, risk_state, last_signal_json, last_entry_block_reason,
                      execution_json, heartbeat_json, lifecycle_json, payload_json
               FROM runs
               WHERE run_id = %s""",
            (run_id,),
        )
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
    category: Optional[str] = None,
    event_type: Optional[str] = None,
    order_id: Optional[str] = None,
    search: Optional[str] = None,
    since_minutes: Optional[int] = Query(None, ge=1, le=10080),
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        clauses = ["run_id = %s"]
        params: list[Any] = [run_id]
        if category:
            clauses.append("category = %s")
            params.append(category)
        if event_type:
            clauses.append("event_type = %s")
            params.append(event_type)
        if order_id:
            clauses.append("order_id = %s")
            params.append(order_id)
        if since_minutes is not None:
            clauses.append("event_timestamp >= NOW() - (%s * INTERVAL '1 minute')")
            params.append(since_minutes)
        if start_time:
            clauses.append("event_timestamp >= %s")
            params.append(start_time)
        if end_time:
            clauses.append("event_timestamp <= %s")
            params.append(end_time)
        if search:
            clauses.append("(reason ILIKE %s OR action ILIKE %s OR symbol ILIKE %s OR zone ILIKE %s OR payload_json::text ILIKE %s OR event_type ILIKE %s OR source ILIKE %s)")
            pattern = f"%{search}%"
            params.extend([pattern, pattern, pattern, pattern, pattern, pattern, pattern])
        params.append(limit)
        cur.execute(
            f"""SELECT id, run_id, event_timestamp, inserted_at, category, event_type, source, symbol, zone, action,
                       reason, order_id, risk_state, contracts, order_status, guard_reason, decision_side, decision_price,
                       expected_fill_price, entry_guard_json, unresolved_entry_json, execution_json, payload_json
                FROM events
                WHERE {' AND '.join(clauses)}
                ORDER BY event_timestamp DESC, id DESC
                LIMIT %s""",
            tuple(params),
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
            """SELECT id, run_id, inserted_at, entry_time, exit_time, direction, contracts, entry_price, exit_price, pnl,
                      zone, strategy, regime, event_tags_json, source, backfilled, payload_json
               FROM completed_trades
               WHERE run_id = %s
               ORDER BY COALESCE(exit_time, inserted_at) DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {"trades": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/state_snapshots")
async def run_state_snapshots(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(100, ge=1, le=500),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, run_id, captured_at, status, data_mode, symbol, zone, zone_state, position, position_pnl,
                      daily_pnl, risk_state, last_signal_json, last_entry_reason, last_entry_block_reason, decision_price,
                      entry_guard_json, unresolved_entry_json, execution_json, heartbeat_json, lifecycle_json,
                      observability_json, payload_json
               FROM state_snapshots
               WHERE run_id = %s
               ORDER BY captured_at DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {"stateSnapshots": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/timeline")
async def run_timeline(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(400, ge=1, le=1000),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, run_id, event_timestamp, inserted_at, category, event_type, source, symbol, zone, action,
                      reason, order_id, risk_state, contracts, order_status, guard_reason, decision_side, decision_price,
                      expected_fill_price, entry_guard_json, unresolved_entry_json, execution_json, payload_json
               FROM events
               WHERE run_id = %s
               ORDER BY event_timestamp DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        events = cur.fetchall()
        cur.execute(
            """SELECT id, run_id, captured_at, status, data_mode, symbol, zone, zone_state, position, position_pnl,
                      daily_pnl, risk_state, last_signal_json, last_entry_reason, last_entry_block_reason, decision_price,
                      entry_guard_json, unresolved_entry_json, execution_json, heartbeat_json, lifecycle_json,
                      observability_json, payload_json
               FROM state_snapshots
               WHERE run_id = %s
               ORDER BY captured_at DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        snapshots = cur.fetchall()
        cur.execute(
            """SELECT id, run_id, inserted_at, entry_time, exit_time, direction, contracts, entry_price, exit_price, pnl,
                      zone, strategy, regime, event_tags_json, source, backfilled, payload_json
               FROM completed_trades
               WHERE run_id = %s
               ORDER BY COALESCE(exit_time, inserted_at) DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        trades = cur.fetchall()
    finally:
        put_conn(conn)

    timeline: list[dict[str, Any]] = []
    for row in events:
        item = dict(row)
        item["kind"] = "event"
        item["timestamp"] = _iso(item.pop("event_timestamp"))
        item["inserted_at"] = _iso(item.get("inserted_at"))
        timeline.append(item)
    for row in snapshots:
        item = dict(row)
        item["kind"] = "state_snapshot"
        item["timestamp"] = _iso(item.pop("captured_at"))
        timeline.append(item)
    for row in trades:
        item = dict(row)
        item["kind"] = "trade"
        item["timestamp"] = _iso(item.get("exit_time") or item.get("inserted_at"))
        item["inserted_at"] = _iso(item.get("inserted_at"))
        timeline.append(item)

    timeline.sort(key=lambda item: item.get("timestamp") or "", reverse=True)
    blockers = [
        item
        for item in timeline
        if item.get("kind") == "event" and (
            item.get("event_type") in {"trade_blocked", "risk_state_changed", "fail_safe_activated", "duplicate_unresolved_entry_detected", "unresolved_entry_tracked", "unresolved_entry_cleared"}
            or item.get("guard_reason")
            or item.get("reason")
        )
    ]
    return {
        "run_id": run_id,
        "timeline": timeline,
        "blockers": blockers,
        "counts": {
            "events": len(events),
            "state_snapshots": len(snapshots),
            "trades": len(trades),
        },
    }


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
        cur.execute("SELECT COUNT(*) AS state_snapshot_count, MAX(captured_at) AS latest_state_at FROM state_snapshots")
        snapshots = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS trade_count, COALESCE(SUM(pnl), 0) AS total_pnl FROM completed_trades")
        row = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS report_count, MAX(created_at) AS latest_report_at FROM ai_reports")
        reports = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS blocker_count FROM events WHERE event_type IN ('trade_blocked', 'risk_state_changed', 'fail_safe_activated', 'duplicate_unresolved_entry_detected', 'unresolved_entry_tracked', 'unresolved_entry_cleared')")
        blockers = cur.fetchone()
        cur.execute("SELECT MAX(last_seen_at) AS latest_run_seen_at FROM runs")
        latest_run_seen = cur.fetchone()
        return {
            "run_count": runs,
            "event_count": events,
            "state_snapshot_count": snapshots["state_snapshot_count"],
            "latest_state_at": _iso(snapshots.get("latest_state_at")),
            "trade_count": row["trade_count"],
            "total_pnl": float(row["total_pnl"] or 0),
            "report_count": reports["report_count"],
            "latest_report_at": _iso(reports.get("latest_report_at")),
            "blocker_count": blockers["blocker_count"],
            "latest_run_seen_at": _iso(latest_run_seen.get("latest_run_seen_at")),
        }
    finally:
        put_conn(conn)


@app.get("/health")
async def health():
    pool_health = check_pool_health()
    return {"status": "ok", "pool": pool_health}


# GraphQL (advisory-only mutations; Bearer auth via info.context["request"])
from strawberry.fastapi import GraphQLRouter
from graphql_schema import schema

graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8001")))
