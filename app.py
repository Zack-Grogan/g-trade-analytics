"""
g-trade-analytics: G-Trade analytics service; query-only API over Postgres.
Auth: Bearer ANALYTICS_API_KEY or GTRADE_INTERNAL_API_TOKEN.
"""
from __future__ import annotations

import os
import logging
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Optional, Generator

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, RealDictConnection

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
INTERNAL_API_TOKEN = (os.environ.get("GTRADE_INTERNAL_API_TOKEN") or "").strip()
ANALYTICS_API_KEY = os.environ.get("ANALYTICS_API_KEY", "")

_pool: ThreadedConnectionPool | None = None
_schema_lock = threading.Lock()
_schema_ready = False
_last_mv_refresh_at = 0.0
_MV_REFRESH_INTERVAL_SECONDS = 15.0
_DERIVED_BASE_TABLES = (
    "runs",
    "events",
    "state_snapshots",
    "market_tape",
    "decision_snapshots",
    "order_lifecycle",
    "completed_trades",
)
_REQUIRED_MV_RUN_SUMMARIES_COLUMNS = {
    "run_id",
    "created_at",
    "last_seen_at",
    "account_id",
    "account_name",
    "account_mode",
    "account_is_practice",
    "event_count",
    "state_snapshot_count",
    "decision_snapshot_count",
    "market_tape_count",
    "order_lifecycle_count",
    "trade_count",
    "runtime_log_count",
    "total_pnl",
    "latest_state_at",
    "latest_decision_at",
    "latest_market_at",
    "latest_order_at",
    "latest_runtime_log_at",
    "latest_event_at",
    "latest_trade_at",
    "latest_blocker_at",
}
_REQUIRED_V_RUN_TIMELINE_COLUMNS = {
    "run_id",
    "timeline_at",
    "kind",
    "category",
    "event_type",
    "source",
    "symbol",
    "zone",
    "action",
    "reason",
    "order_id",
    "risk_state",
    "payload_json",
}


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


def _runtime_log_filters(
    *,
    run_id: Optional[str] = None,
    level: Optional[str] = None,
    search: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if run_id:
        clauses.append("run_id = %s")
        params.append(run_id)
    if level:
        clauses.append("level = %s")
        params.append(level)
    if start_time:
        clauses.append("logged_at >= %s")
        params.append(start_time)
    if end_time:
        clauses.append("logged_at <= %s")
        params.append(end_time)
    if search:
        pattern = f"%{search}%"
        clauses.append(
            "(message ILIKE %s OR logger_name ILIKE %s OR source ILIKE %s OR service_name ILIKE %s OR payload_json::text ILIKE %s)"
        )
        params.extend([pattern, pattern, pattern, pattern, pattern])
    return clauses, params


def check_pool_health() -> dict[str, Any]:
    """Check connection pool health and return stats."""
    pool = _get_pool()
    available = len(getattr(pool, "_pool", []))
    borrowed = len(getattr(pool, "_used", {}))
    return {
        "pool_minconn": getattr(pool, "minconn", None),
        "pool_maxconn": getattr(pool, "maxconn", None),
        "pool_available": available,
        "pool_borrowed": borrowed,
        "pool_closed": bool(getattr(pool, "closed", False)),
    }


def _existing_relations(cur) -> dict[str, bool]:
    cur.execute(
        """
        SELECT relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND relname = ANY(%s)
        """,
        (
            list(_DERIVED_BASE_TABLES) + ["mv_run_summaries", "mv_daily_trade_stats", "v_run_timeline"],
        ),
    )
    return {row["relname"]: True for row in cur.fetchall()}


def _relation_columns(cur, relation_name: str) -> set[str]:
    cur.execute(
        """
        SELECT a.attname
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        """,
        (relation_name,),
    )
    return {row["attname"] for row in cur.fetchall()}


def _base_tables_ready(cur) -> bool:
    relations = _existing_relations(cur)
    if not all(relations.get(name) for name in _DERIVED_BASE_TABLES):
        missing = [name for name in _DERIVED_BASE_TABLES if not relations.get(name)]
        logger.info("Analytics schema ensure deferred; base tables not ready: %s", ", ".join(missing))
        return False
    return True


def _derived_relations_ready(cur) -> bool:
    relations = _existing_relations(cur)
    if not relations.get("mv_run_summaries") or not relations.get("v_run_timeline"):
        return False
    mv_columns = _relation_columns(cur, "mv_run_summaries")
    if not _REQUIRED_MV_RUN_SUMMARIES_COLUMNS.issubset(mv_columns):
        return False
    timeline_columns = _relation_columns(cur, "v_run_timeline")
    if not _REQUIRED_V_RUN_TIMELINE_COLUMNS.issubset(timeline_columns):
        return False
    return True


def _refresh_materialized_views(cur) -> None:
    cur.execute("REFRESH MATERIALIZED VIEW mv_run_summaries")
    cur.execute("REFRESH MATERIALIZED VIEW mv_daily_trade_stats")


def ensure_schema_v2(force_recreate: bool = False) -> bool:
    """Apply RLM/analytics extension schema (hypotheses, experiments, knowledge_store, trade_embeddings, memory graph)."""
    global _schema_ready, _last_mv_refresh_at
    schema_path = os.path.join(os.path.dirname(__file__), "schema_v2.sql")
    if not os.path.exists(schema_path):
        return False
    with _schema_lock:
        conn = get_conn()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            if not _base_tables_ready(cur):
                return False
            relations_ready = _derived_relations_ready(cur)
            if not relations_ready or force_recreate:
                cur.execute("DROP VIEW IF EXISTS v_run_timeline")
                cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_daily_trade_stats")
                cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_run_summaries")
                with open(schema_path) as f:
                    cur.execute(f.read())
                conn.commit()
                _schema_ready = True
                _last_mv_refresh_at = time.monotonic()
                logger.info("Analytics derived schema rebuilt")
                return True
            now = time.monotonic()
            if now - _last_mv_refresh_at >= _MV_REFRESH_INTERVAL_SECONDS:
                _refresh_materialized_views(cur)
                conn.commit()
                _last_mv_refresh_at = now
            _schema_ready = True
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            put_conn(conn)


def ensure_derived_read_models(force_recreate: bool = False) -> None:
    try:
        ensure_schema_v2(force_recreate=force_recreate)
    except Exception as exc:
        logger.warning("Derived analytics schema ensure failed: %s", exc)


def _bearer_ok(authorization: str | None) -> bool:
    if not authorization or not authorization.startswith("Bearer "):
        return False
    token = authorization[7:].strip()
    allowed = [candidate for candidate in (INTERNAL_API_TOKEN, ANALYTICS_API_KEY.strip()) if candidate]
    return bool(allowed) and token in allowed


def _require_auth(authorization: str | None) -> None:
    if not _bearer_ok(authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing Bearer token")


def _require_graphql_auth(authorization: str | None = Header(None)) -> None:
    _require_auth(authorization)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        ensure_derived_read_models()
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
    ensure_derived_read_models()
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if search:
            cur.execute(
                """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol, status, zone, zone_state,
                          position, position_pnl, daily_pnl, risk_state, event_count, state_snapshot_count,
                          decision_snapshot_count, market_tape_count, order_lifecycle_count, trade_count, total_pnl,
                          account_id, account_name, account_mode, account_is_practice,
                          latest_state_at, latest_decision_at, latest_market_at, latest_order_at, latest_event_at,
                          latest_trade_at, latest_blocker_at
                   FROM mv_run_summaries
                   WHERE run_id ILIKE %s
                      OR data_mode ILIKE %s
                      OR status ILIKE %s
                      OR zone ILIKE %s
                      OR risk_state ILIKE %s
                      OR symbol ILIKE %s
                      OR account_name ILIKE %s
                   ORDER BY COALESCE(last_seen_at, created_at) DESC
                   LIMIT %s""",
                (f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", limit),
            )
        else:
            cur.execute(
                """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol, status, zone, zone_state,
                          position, position_pnl, daily_pnl, risk_state, event_count, state_snapshot_count,
                          decision_snapshot_count, market_tape_count, order_lifecycle_count, trade_count, total_pnl,
                          account_id, account_name, account_mode, account_is_practice,
                          latest_state_at, latest_decision_at, latest_market_at, latest_order_at, latest_event_at,
                          latest_trade_at, latest_blocker_at
                   FROM mv_run_summaries
                   ORDER BY COALESCE(last_seen_at, created_at) DESC
                   LIMIT %s""",
                (limit,),
            )
        rows = cur.fetchall()
        return {"runs": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/compare")
async def compare_runs(
    authorization: str | None = Header(None),
    left_run_id: str = Query(...),
    right_run_id: str = Query(...),
):
    _require_auth(authorization)
    ensure_derived_read_models()
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM mv_run_summaries WHERE run_id = %s", (left_run_id,))
        left = cur.fetchone()
        cur.execute("SELECT * FROM mv_run_summaries WHERE run_id = %s", (right_run_id,))
        right = cur.fetchone()
        if not left or not right:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        left_dict = dict(left)
        right_dict = dict(right)
        return {
            "left_run_id": left_run_id,
            "right_run_id": right_run_id,
            "left": left_dict,
            "right": right_dict,
            "delta": {
                "event_count": int(right_dict.get("event_count") or 0) - int(left_dict.get("event_count") or 0),
                "state_snapshot_count": int(right_dict.get("state_snapshot_count") or 0) - int(left_dict.get("state_snapshot_count") or 0),
                "decision_snapshot_count": int(right_dict.get("decision_snapshot_count") or 0) - int(left_dict.get("decision_snapshot_count") or 0),
                "market_tape_count": int(right_dict.get("market_tape_count") or 0) - int(left_dict.get("market_tape_count") or 0),
                "order_lifecycle_count": int(right_dict.get("order_lifecycle_count") or 0) - int(left_dict.get("order_lifecycle_count") or 0),
                "trade_count": int(right_dict.get("trade_count") or 0) - int(left_dict.get("trade_count") or 0),
                "total_pnl": float(right_dict.get("total_pnl") or 0) - float(left_dict.get("total_pnl") or 0),
            },
        }
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}")
async def get_run(
    run_id: str,
    authorization: str | None = Header(None),
):
    _require_auth(authorization)
    ensure_derived_read_models()
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT r.run_id, r.created_at, r.last_seen_at, r.process_id, r.data_mode, r.symbol, r.status, r.zone,
                      r.zone_state, r.position, r.position_pnl, r.daily_pnl, r.risk_state, r.account_id, r.account_name,
                      r.account_mode, r.account_is_practice, r.last_signal_json,
                      r.last_entry_block_reason, r.execution_json, r.heartbeat_json, r.lifecycle_json, r.payload_json,
                      s.event_count, s.state_snapshot_count, s.decision_snapshot_count, s.market_tape_count,
                      s.order_lifecycle_count, s.trade_count, s.total_pnl, s.latest_state_at, s.latest_decision_at,
                      s.latest_market_at, s.latest_order_at, s.latest_event_at, s.latest_trade_at, s.latest_blocker_at
               FROM runs r
               LEFT JOIN mv_run_summaries s ON s.run_id = r.run_id
               WHERE r.run_id = %s""",
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
                      zone, strategy, regime, event_tags_json, source, backfilled, trade_id, position_id, decision_id,
                      attempt_id, account_id, account_name, account_mode, account_is_practice, payload_json
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
                      daily_pnl, risk_state, account_id, account_name, account_mode, account_is_practice,
                      last_signal_json, last_entry_reason, last_entry_block_reason, decision_price,
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


@app.get("/runs/{run_id}/market_tape")
async def run_market_tape(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(500, ge=1, le=2000),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, run_id, captured_at, inserted_at, process_id, symbol, contract_id, bid, ask, last, volume,
                      bid_size, ask_size, last_size, volume_is_cumulative, quote_is_synthetic, trade_side, latency_ms,
                      source, sequence, payload_json
               FROM market_tape
               WHERE run_id = %s
               ORDER BY captured_at DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {"marketTape": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/decision_snapshots")
async def run_decision_snapshots(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(200, ge=1, le=2000),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, run_id, decided_at, inserted_at, decision_id, attempt_id, process_id, symbol, zone,
                      action, reason, outcome, outcome_reason, long_score, short_score, flat_bias, score_gap,
                      dominant_side, current_price, allow_entries, execution_tradeable, contracts, order_type,
                      limit_price, decision_price, side, stop_loss, take_profit, max_hold_minutes, regime_state,
                      regime_reason, active_session, active_vetoes_json, feature_snapshot_json, entry_guard_json,
                      unresolved_entry_json, event_context_json, order_flow_json, payload_json
               FROM decision_snapshots
               WHERE run_id = %s
               ORDER BY decided_at DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {"decisionSnapshots": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/order_lifecycle")
async def run_order_lifecycle(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(500, ge=1, le=5000),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, run_id, observed_at, inserted_at, decision_id, attempt_id, order_id, position_id, trade_id,
                      process_id, symbol, event_type, status, side, role, is_protective, order_type, quantity, contracts,
                      limit_price, stop_price, expected_fill_price, filled_price, filled_quantity, remaining_quantity,
                      zone, reason, lifecycle_state, payload_json
               FROM order_lifecycle
               WHERE run_id = %s
               ORDER BY observed_at DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {"orderLifecycle": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/bridge_health")
async def run_bridge_health(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(100, ge=1, le=1000),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, run_id, observed_at, bridge_status, queue_depth, last_flush_at, last_success_at, last_error,
                      payload_json
               FROM bridge_ingest_health
               WHERE run_id = %s
               ORDER BY observed_at DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {"bridgeHealth": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/runtime_logs")
async def run_runtime_logs(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(200, ge=1, le=2000),
    level: Optional[str] = None,
    search: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        clauses, params = _runtime_log_filters(
            run_id=run_id,
            level=level,
            search=search,
            start_time=start_time,
            end_time=end_time,
        )
        params.append(limit)
        cur.execute(
            f"""SELECT id, run_id, logged_at, inserted_at, level, logger_name, source, service_name, process_id,
                       line_hash, message, payload_json
                FROM runtime_logs
                WHERE {' AND '.join(clauses)}
                ORDER BY logged_at DESC, id DESC
                LIMIT %s""",
            tuple(params),
        )
        rows = cur.fetchall()
        return {"runtimeLogs": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/manifest")
async def run_manifest(
    run_id: str,
    authorization: str | None = Header(None),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol, account_id, account_name,
                      account_mode, account_is_practice, config_path, config_hash,
                      log_path, sqlite_path, git_commit, git_branch, git_dirty, git_available, app_version, payload_json
               FROM run_manifests
               WHERE run_id = %s""",
            (run_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run manifest not found")
        return dict(row)
    finally:
        put_conn(conn)


@app.get("/accounts")
async def account_summaries(
    authorization: str | None = Header(None),
    limit: int = Query(20, ge=1, le=100),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            WITH runs_agg AS (
                SELECT
                    account_id,
                    MAX(account_name) AS account_name,
                    MAX(account_mode) AS account_mode,
                    BOOL_OR(COALESCE(account_is_practice, false)) AS account_is_practice,
                    COUNT(*) AS run_count,
                    MAX(last_seen_at) AS latest_run_seen_at
                FROM runs
                WHERE account_id IS NOT NULL
                GROUP BY account_id
            ),
            trade_agg AS (
                SELECT
                    account_id,
                    MAX(account_name) AS account_name,
                    MAX(account_mode) AS account_mode,
                    BOOL_OR(COALESCE(account_is_practice, false)) AS account_is_practice,
                    COUNT(*) AS trade_count,
                    COALESCE(SUM(profit_and_loss), 0) AS realized_pnl,
                    MAX(occurred_at) AS latest_trade_at
                FROM account_trades
                GROUP BY account_id
            )
            SELECT
                COALESCE(r.account_id, t.account_id) AS account_id,
                COALESCE(r.account_name, t.account_name) AS account_name,
                COALESCE(r.account_mode, t.account_mode) AS account_mode,
                COALESCE(r.account_is_practice, t.account_is_practice) AS account_is_practice,
                COALESCE(r.run_count, 0) AS run_count,
                COALESCE(t.trade_count, 0) AS trade_count,
                COALESCE(t.realized_pnl, 0) AS realized_pnl,
                t.latest_trade_at,
                r.latest_run_seen_at
            FROM runs_agg r
            FULL OUTER JOIN trade_agg t ON t.account_id = r.account_id
            ORDER BY COALESCE(t.latest_trade_at, r.latest_run_seen_at) DESC
            LIMIT %s
            """,
            (limit,),
        )
        return {"accounts": [dict(row) for row in cur.fetchall()]}
    finally:
        put_conn(conn)


@app.get("/account-trades")
async def account_trades(
    authorization: str | None = Header(None),
    limit: int = Query(100, ge=1, le=500),
    account_id: Optional[str] = None,
    run_id: Optional[str] = None,
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        clauses: list[str] = []
        params: list[Any] = []
        if account_id:
            clauses.append("account_id = %s")
            params.append(account_id)
        if run_id:
            clauses.append("run_id = %s")
            params.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        cur.execute(
            f"""
            SELECT id, run_id, inserted_at, occurred_at, account_id, account_name, account_mode, account_is_practice,
                   broker_trade_id, broker_order_id, contract_id, side, size, price, profit_and_loss, fees,
                   voided, source, payload_json
            FROM account_trades
            {where}
            ORDER BY occurred_at DESC, id DESC
            LIMIT %s
            """,
            tuple(params),
        )
        return {"accountTrades": [dict(row) for row in cur.fetchall()]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/non_entry_explanations")
async def run_non_entry_explanations(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(200, ge=1, le=1000),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT reason, outcome, outcome_reason, action, decision_id, attempt_id, zone, zone_state, symbol,
                      action AS decision_action, long_score, short_score, flat_bias, score_gap, payload_json, decided_at
               FROM decision_snapshots
               WHERE run_id = %s
                 AND COALESCE(outcome, '') <> 'order_submitted'
               ORDER BY decided_at DESC, id DESC
               LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
        return {
            "run_id": run_id,
            "explanations": [dict(r) for r in rows],
            "counts": {
                "total": len(rows),
                "blocked": sum(1 for row in rows if (row.get("outcome") or "").lower() not in {"", "order_submitted"}),
            },
        }
    finally:
        put_conn(conn)


@app.get("/search/runs")
async def search_runs(
    authorization: str | None = Header(None),
    q: str = Query(..., min_length=1),
    limit: int = Query(25, ge=1, le=100),
):
    _require_auth(authorization)
    ensure_derived_read_models()
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol, status, zone, zone_state,
                      position, position_pnl, daily_pnl, risk_state, event_count, state_snapshot_count,
                      decision_snapshot_count, market_tape_count, order_lifecycle_count, trade_count, total_pnl,
                      account_id, account_name, account_mode, account_is_practice,
                      latest_state_at, latest_decision_at, latest_market_at, latest_order_at, latest_event_at,
                      latest_trade_at, latest_blocker_at
               FROM mv_run_summaries
               WHERE run_id ILIKE %s OR data_mode ILIKE %s OR status ILIKE %s OR zone ILIKE %s OR risk_state ILIKE %s
                  OR symbol ILIKE %s OR account_name ILIKE %s
               ORDER BY COALESCE(last_seen_at, created_at) DESC
               LIMIT %s""",
            (f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", limit),
        )
        rows = cur.fetchall()
        return {"runs": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/search/events")
async def search_events(
    authorization: str | None = Header(None),
    q: str = Query(..., min_length=1),
    limit: int = Query(100, ge=1, le=500),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        pattern = f"%{q}%"
        cur.execute(
            """SELECT id, run_id, event_timestamp, inserted_at, category, event_type, source, symbol, zone, action,
                      reason, order_id, risk_state, contracts, order_status, guard_reason, decision_side, decision_price,
                      expected_fill_price, entry_guard_json, unresolved_entry_json, execution_json, payload_json
               FROM events
               WHERE reason ILIKE %s
                  OR action ILIKE %s
                  OR symbol ILIKE %s
                  OR zone ILIKE %s
                  OR source ILIKE %s
                  OR event_type ILIKE %s
                  OR payload_json::text ILIKE %s
               ORDER BY event_timestamp DESC, id DESC
               LIMIT %s""",
            (pattern, pattern, pattern, pattern, pattern, pattern, pattern, limit),
        )
        rows = cur.fetchall()
        return {"events": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/search/runtime_logs")
async def search_runtime_logs(
    authorization: str | None = Header(None),
    q: str = Query(..., min_length=1),
    run_id: Optional[str] = None,
    level: Optional[str] = None,
    limit: int = Query(200, ge=1, le=2000),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        clauses, params = _runtime_log_filters(run_id=run_id, level=level, search=q)
        params.append(limit)
        where = " AND ".join(clauses) if clauses else "TRUE"
        cur.execute(
            f"""SELECT id, run_id, logged_at, inserted_at, level, logger_name, source, service_name, process_id,
                       line_hash, message, payload_json
                FROM runtime_logs
                WHERE {where}
                ORDER BY logged_at DESC, id DESC
                LIMIT %s""",
            tuple(params),
        )
        rows = cur.fetchall()
        return {"runtimeLogs": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/runs/{run_id}/timeline")
async def run_timeline(
    run_id: str,
    authorization: str | None = Header(None),
    limit: int = Query(400, ge=1, le=1000),
):
    _require_auth(authorization)
    ensure_derived_read_models()
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT *
               FROM v_run_timeline
               WHERE run_id = %s
               ORDER BY timeline_at DESC
               LIMIT %s""",
            (run_id, limit),
        )
        rows = cur.fetchall()
    finally:
        put_conn(conn)

    timeline: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["timestamp"] = _iso(item.get("timeline_at"))
        timeline.append(item)
    blockers = [
        item
        for item in timeline
        if (
            item.get("kind") in {"event", "decision_snapshot", "order_lifecycle"}
            and (
                item.get("event_type") in {"trade_blocked", "risk_state_changed", "fail_safe_activated", "duplicate_unresolved_entry_detected", "unresolved_entry_tracked", "unresolved_entry_cleared"}
                or item.get("guard_reason")
                or item.get("reason")
                or item.get("outcome_reason")
            )
        )
    ]
    return {
        "run_id": run_id,
        "timeline": timeline,
        "blockers": blockers,
        "counts": {
            "timeline_entries": len(rows),
            "blockers": len(blockers),
        },
    }


@app.get("/bridge/failures")
async def bridge_failures(
    authorization: str | None = Header(None),
    run_id: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        clauses = ["(COALESCE(last_error, '') <> '' OR COALESCE(bridge_status, '') NOT IN ('running', 'healthy', 'ok'))"]
        params: list[Any] = []
        if run_id:
            clauses.append("run_id = %s")
            params.append(run_id)
        params.append(limit)
        cur.execute(
            f"""SELECT id, run_id, observed_at, bridge_status, queue_depth, last_flush_at, last_success_at, last_error,
                       payload_json
                FROM bridge_ingest_health
                WHERE {' AND '.join(clauses)}
                ORDER BY observed_at DESC, id DESC
                LIMIT %s""",
            tuple(params),
        )
        rows = cur.fetchall()
        return {"failures": [dict(r) for r in rows]}
    finally:
        put_conn(conn)


@app.get("/analytics/summary")
async def analytics_summary(
    authorization: str | None = Header(None),
):
    _require_auth(authorization)
    ensure_derived_read_models()
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT
                   COUNT(*) AS run_count,
                   COALESCE(SUM(event_count), 0) AS event_count,
                   COALESCE(SUM(state_snapshot_count), 0) AS state_snapshot_count,
                   COALESCE(SUM(decision_snapshot_count), 0) AS decision_snapshot_count,
                   COALESCE(SUM(market_tape_count), 0) AS market_tape_count,
                   COALESCE(SUM(order_lifecycle_count), 0) AS order_lifecycle_count,
                   COALESCE(SUM(runtime_log_count), 0) AS runtime_log_count,
                   COALESCE(SUM(trade_count), 0) AS trade_count,
                   COALESCE(SUM(total_pnl), 0) AS total_pnl,
                   MAX(latest_state_at) AS latest_state_at,
                   MAX(latest_decision_at) AS latest_decision_at,
                   MAX(latest_market_at) AS latest_market_at,
                   MAX(latest_order_at) AS latest_order_at,
                   MAX(latest_runtime_log_at) AS latest_runtime_log_at,
                   MAX(latest_event_at) AS latest_event_at,
                   MAX(latest_trade_at) AS latest_trade_at,
                   MAX(latest_blocker_at) AS latest_blocker_at,
                   MAX(last_seen_at) AS latest_run_seen_at
               FROM mv_run_summaries"""
        )
        summary = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS report_count, MAX(created_at) AS latest_report_at FROM ai_reports")
        reports = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS blocker_count FROM events WHERE event_type IN ('trade_blocked', 'risk_state_changed', 'fail_safe_activated', 'duplicate_unresolved_entry_detected', 'unresolved_entry_tracked', 'unresolved_entry_cleared')")
        blockers = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS bridge_failure_count, MAX(observed_at) AS latest_bridge_failure_at FROM bridge_ingest_health WHERE COALESCE(last_error, '') <> ''")
        bridge_failures_row = cur.fetchone()
        return {
            "run_count": summary["run_count"],
            "event_count": summary["event_count"],
            "state_snapshot_count": summary["state_snapshot_count"],
            "decision_snapshot_count": summary["decision_snapshot_count"],
            "market_tape_count": summary["market_tape_count"],
            "order_lifecycle_count": summary["order_lifecycle_count"],
            "runtime_log_count": summary["runtime_log_count"],
            "latest_state_at": _iso(summary.get("latest_state_at")),
            "latest_decision_at": _iso(summary.get("latest_decision_at")),
            "latest_market_at": _iso(summary.get("latest_market_at")),
            "latest_order_at": _iso(summary.get("latest_order_at")),
            "latest_runtime_log_at": _iso(summary.get("latest_runtime_log_at")),
            "latest_event_at": _iso(summary.get("latest_event_at")),
            "latest_trade_at": _iso(summary.get("latest_trade_at")),
            "latest_blocker_at": _iso(summary.get("latest_blocker_at")),
            "trade_count": summary["trade_count"],
            "total_pnl": float(summary["total_pnl"] or 0),
            "report_count": reports["report_count"],
            "latest_report_at": _iso(reports.get("latest_report_at")),
            "blocker_count": blockers["blocker_count"],
            "bridge_failure_count": bridge_failures_row["bridge_failure_count"],
            "latest_bridge_failure_at": _iso(bridge_failures_row.get("latest_bridge_failure_at")),
            "latest_run_seen_at": _iso(summary.get("latest_run_seen_at")),
        }
    finally:
        put_conn(conn)


@app.get("/service-health")
async def service_health(
    authorization: str | None = Header(None),
):
    _require_auth(authorization)
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT run_id, observed_at, bridge_status, queue_depth, last_success_at, last_error
               FROM bridge_ingest_health
               ORDER BY observed_at DESC, id DESC
               LIMIT 1"""
        )
        latest_bridge = cur.fetchone()
        cur.execute(
            """SELECT COUNT(*) AS runtime_log_count, MAX(logged_at) AS latest_runtime_log_at
               FROM runtime_logs"""
        )
        runtime_logs = cur.fetchone()
        cur.execute(
            """SELECT COUNT(*) AS report_count, MAX(created_at) AS latest_report_at
               FROM ai_reports"""
        )
        reports = cur.fetchone()
        return {
            "analytics": {"status": "ok", "pool": check_pool_health()},
            "bridge": dict(latest_bridge) if latest_bridge else None,
            "runtime_logs": {
                "count": runtime_logs["runtime_log_count"],
                "latest_logged_at": _iso(runtime_logs.get("latest_runtime_log_at")),
            },
            "reports": {
                "count": reports["report_count"],
                "latest_created_at": _iso(reports.get("latest_report_at")),
            },
        }
    finally:
        put_conn(conn)


@app.get("/health")
async def health():
    pool_health = check_pool_health()
    return {"status": "ok", "pool": pool_health}


# GraphQL query surface (read-only; Bearer auth via dependency and info.context["request"])
from strawberry.fastapi import GraphQLRouter
from graphql_schema import schema

graphql_app = GraphQLRouter(schema, dependencies=[Depends(_require_graphql_auth)])
app.include_router(graphql_app, prefix="/graphql")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8001")))
