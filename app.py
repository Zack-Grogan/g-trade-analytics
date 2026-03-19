"""
g-trade-analytics: G-Trade analytics service; query-only API over Postgres.
Auth: Bearer ANALYTICS_API_KEY or GTRADE_INTERNAL_API_TOKEN.
"""
from __future__ import annotations

import os
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
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


def _parse_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _format_minutes(delta: timedelta | None) -> str | None:
    if delta is None:
        return None
    total_minutes = delta.total_seconds() / 60.0
    return f"{total_minutes:.1f}m"


def _trade_direction_label(direction: Any) -> str:
    try:
        value = int(direction)
    except (TypeError, ValueError):
        return "flat"
    if value > 0:
        return "long"
    if value < 0:
        return "short"
    return "flat"


def _as_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result:  # NaN guard
        return None
    return result


def _trade_window_bounds(trade: dict[str, Any], padding_minutes: int = 5) -> tuple[datetime | None, datetime | None]:
    anchors = [
        _parse_dt(trade.get("entry_time")),
        _parse_dt(trade.get("exit_time")),
        _parse_dt(trade.get("inserted_at")),
    ]
    anchors = [anchor for anchor in anchors if anchor is not None]
    if not anchors:
        return None, None
    start = min(anchors) - timedelta(minutes=padding_minutes)
    end = max(anchors) + timedelta(minutes=padding_minutes)
    return start, end


def _trade_review_summary(trade: dict[str, Any]) -> str:
    direction = _trade_direction_label(trade.get("direction"))
    contracts = int(trade.get("contracts") or 0)
    zone = trade.get("zone") or "unknown zone"
    strategy = trade.get("strategy") or "unknown strategy"
    return (
        f"{direction.title()} {contracts} contract{'s' if contracts != 1 else ''} "
        f"in {zone} via {strategy}; entry {trade.get('entry_price')}, "
        f"exit {trade.get('exit_price')}, pnl {trade.get('pnl')}."
    )


def _trade_marker(trade: dict[str, Any], kind: str) -> dict[str, Any]:
    time_key = "entry_time" if kind == "entry" else "exit_time"
    price_key = "entry_price" if kind == "entry" else "exit_price"
    return {
        "kind": kind,
        "timestamp": _iso(trade.get(time_key)),
        "price": trade.get(price_key),
        "direction": _trade_direction_label(trade.get("direction")),
        "contracts": trade.get("contracts"),
        "zone": trade.get("zone"),
        "strategy": trade.get("strategy"),
        "trade_id": trade.get("trade_id") or trade.get("id"),
        "pnl": trade.get("pnl"),
    }


def _summarize_market_tape(market_tape: list[dict[str, Any]]) -> dict[str, Any]:
    if not market_tape:
        return {
            "summary": "No market tape rows found in the trade window.",
            "tick_count": 0,
            "first_last": None,
            "last_last": None,
            "high_last": None,
            "low_last": None,
            "avg_spread": None,
        }

    last_prices: list[float] = []
    spreads: list[float] = []
    for row in market_tape:
        last_price = _as_float(row.get("last"))
        if last_price is not None:
            last_prices.append(last_price)
        bid = _as_float(row.get("bid"))
        ask = _as_float(row.get("ask"))
        if bid is not None and ask is not None:
            spreads.append(ask - bid)

    first_last = last_prices[0] if last_prices else None
    last_last = last_prices[-1] if last_prices else None
    high_last = max(last_prices) if last_prices else None
    low_last = min(last_prices) if last_prices else None
    avg_spread = (sum(spreads) / len(spreads)) if spreads else None

    summary_parts = [f"{len(market_tape)} ticks"]
    if first_last is not None and last_last is not None:
        summary_parts.append(f"last moved from {first_last:.2f} to {last_last:.2f}")
    if high_last is not None and low_last is not None:
        summary_parts.append(f"range {low_last:.2f}-{high_last:.2f}")
    if avg_spread is not None:
        summary_parts.append(f"avg spread {avg_spread:.2f}")

    return {
        "summary": "; ".join(summary_parts),
        "tick_count": len(market_tape),
        "first_last": first_last,
        "last_last": last_last,
        "high_last": high_last,
        "low_last": low_last,
        "avg_spread": avg_spread,
    }


def _summarize_state_changes(state_snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    if not state_snapshots:
        return {
            "summary": "No state snapshots were captured in the trade window.",
            "before": None,
            "after": None,
        }

    before = state_snapshots[0]
    after = state_snapshots[-1]
    fields = ["position", "position_pnl", "daily_pnl", "risk_state", "zone_state", "last_entry_block_reason"]
    deltas: list[str] = []
    for field in fields:
        before_value = before.get(field)
        after_value = after.get(field)
        if before_value != after_value:
            deltas.append(f"{field} {before_value!r} -> {after_value!r}")

    if not deltas:
        deltas.append("No visible state drift inside the selected window.")

    return {
        "summary": "; ".join(deltas),
        "before": before,
        "after": after,
    }


def _primary_decision_snapshot(
    decision_snapshots: list[dict[str, Any]],
    trade: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if trade is not None:
        for key in ("decision_id", "attempt_id"):
            trade_value = trade.get(key)
            if trade_value in (None, ""):
                continue
            for snapshot in decision_snapshots:
                if snapshot.get(key) == trade_value:
                    return snapshot
    return decision_snapshots[-1] if decision_snapshots else None


def _summarize_executor_view(
    decision_snapshots: list[dict[str, Any]],
    trade: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not decision_snapshots:
        return {
            "summary": "No decision snapshots were captured in the trade window.",
            "primary": None,
            "projection_hints": [],
        }

    primary = _primary_decision_snapshot(decision_snapshots, trade)
    if primary is None:
        return {
            "summary": "No decision snapshots were captured in the trade window.",
            "primary": None,
            "projection_hints": [],
        }
    dominant_side = primary.get("dominant_side") or "unknown"
    score_gap = _as_float(primary.get("score_gap"))
    long_score = _as_float(primary.get("long_score"))
    short_score = _as_float(primary.get("short_score"))
    allow_entries = primary.get("allow_entries")
    execution_tradeable = primary.get("execution_tradeable")
    regime_state = primary.get("regime_state") or "unknown"
    outcome = primary.get("outcome") or "unknown"
    reason = primary.get("reason") or primary.get("outcome_reason") or "no reason recorded"

    summary = (
        f"Dominant side {dominant_side}; scores long={long_score}, short={short_score}, "
        f"gap={score_gap}; entries={allow_entries}; tradeable={execution_tradeable}; "
        f"regime={regime_state}; outcome={outcome}; reason={reason}."
    )

    hints: list[str] = []
    if allow_entries is False:
        hints.append("Entry gate was closed for this decision state.")
    if execution_tradeable is False:
        hints.append("Executor marked the setup non-tradeable.")
    if dominant_side not in (None, "unknown") and score_gap is not None:
        hints.append(f"Projection leaned {dominant_side} with score gap {score_gap:+.2f}.")
    if regime_state not in (None, "unknown"):
        hints.append(f"Regime state was {regime_state}.")
    if not hints:
        hints.append("No strong projection hints were persisted for this decision window.")

    return {
        "summary": summary,
        "primary": primary,
        "projection_hints": hints,
    }


def _build_trade_timeline(
    trade: dict[str, Any],
    market_tape: list[dict[str, Any]],
    decision_snapshots: list[dict[str, Any]],
    state_snapshots: list[dict[str, Any]],
    order_lifecycle: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = [
        {
            "kind": "trade",
            "timestamp": _iso(trade.get("entry_time") or trade.get("exit_time") or trade.get("inserted_at")),
            "run_id": trade.get("run_id"),
            "trade_id": trade.get("trade_id") or trade.get("id"),
            "entry": _trade_marker(trade, "entry"),
            "exit": _trade_marker(trade, "exit"),
            "pnl": trade.get("pnl"),
            "zone": trade.get("zone"),
            "strategy": trade.get("strategy"),
            "regime": trade.get("regime"),
        }
    ]

    for row in market_tape:
        timeline.append(
            {
                "kind": "market_tape",
                "timestamp": _iso(row.get("captured_at")),
                "run_id": row.get("run_id"),
                "symbol": row.get("symbol"),
                "contract_id": row.get("contract_id"),
                "bid": row.get("bid"),
                "ask": row.get("ask"),
                "last": row.get("last"),
                "volume": row.get("volume"),
                "trade_side": row.get("trade_side"),
                "latency_ms": row.get("latency_ms"),
                "source": row.get("source"),
            }
        )

    for row in decision_snapshots:
        timeline.append(
            {
                "kind": "decision_snapshot",
                "timestamp": _iso(row.get("decided_at")),
                "run_id": row.get("run_id"),
                "decision_id": row.get("decision_id"),
                "attempt_id": row.get("attempt_id"),
                "symbol": row.get("symbol"),
                "zone": row.get("zone"),
                "action": row.get("action"),
                "reason": row.get("reason"),
                "outcome": row.get("outcome"),
                "dominant_side": row.get("dominant_side"),
                "score_gap": row.get("score_gap"),
                "allow_entries": row.get("allow_entries"),
                "execution_tradeable": row.get("execution_tradeable"),
                "regime_state": row.get("regime_state"),
                "regime_reason": row.get("regime_reason"),
                "feature_snapshot": row.get("feature_snapshot_json") or row.get("feature_snapshot"),
                "entry_guard": row.get("entry_guard_json") or row.get("entry_guard"),
                "unresolved_entry": row.get("unresolved_entry_json") or row.get("unresolved_entry"),
                "event_context": row.get("event_context_json") or row.get("event_context"),
                "order_flow": row.get("order_flow_json") or row.get("order_flow"),
            }
        )

    for row in state_snapshots:
        timeline.append(
            {
                "kind": "state_snapshot",
                "timestamp": _iso(row.get("captured_at")),
                "run_id": row.get("run_id"),
                "status": row.get("status"),
                "symbol": row.get("symbol"),
                "zone": row.get("zone"),
                "zone_state": row.get("zone_state"),
                "position": row.get("position"),
                "position_pnl": row.get("position_pnl"),
                "daily_pnl": row.get("daily_pnl"),
                "risk_state": row.get("risk_state"),
                "account_id": row.get("account_id"),
                "account_name": row.get("account_name"),
                "account_mode": row.get("account_mode"),
                "account_is_practice": row.get("account_is_practice"),
                "last_entry_reason": row.get("last_entry_reason"),
                "last_entry_block_reason": row.get("last_entry_block_reason"),
                "decision_price": row.get("decision_price"),
                "entry_guard": row.get("entry_guard_json") or row.get("entry_guard"),
                "unresolved_entry": row.get("unresolved_entry_json") or row.get("unresolved_entry"),
                "execution_json": row.get("execution_json"),
                "lifecycle_json": row.get("lifecycle_json"),
            }
        )

    for row in order_lifecycle:
        timeline.append(
            {
                "kind": "order_lifecycle",
                "timestamp": _iso(row.get("observed_at")),
                "run_id": row.get("run_id"),
                "decision_id": row.get("decision_id"),
                "attempt_id": row.get("attempt_id"),
                "order_id": row.get("order_id"),
                "position_id": row.get("position_id"),
                "trade_id": row.get("trade_id"),
                "symbol": row.get("symbol"),
                "event_type": row.get("event_type"),
                "status": row.get("status"),
                "side": row.get("side"),
                "role": row.get("role"),
                "is_protective": row.get("is_protective"),
                "order_type": row.get("order_type"),
                "quantity": row.get("quantity"),
                "contracts": row.get("contracts"),
                "limit_price": row.get("limit_price"),
                "stop_price": row.get("stop_price"),
                "filled_price": row.get("filled_price"),
                "filled_quantity": row.get("filled_quantity"),
                "remaining_quantity": row.get("remaining_quantity"),
                "zone": row.get("zone"),
                "reason": row.get("reason"),
                "lifecycle_state": row.get("lifecycle_state"),
            }
        )

    for row in events:
        timeline.append(
            {
                "kind": "event",
                "timestamp": _iso(row.get("event_timestamp")),
                "run_id": row.get("run_id"),
                "category": row.get("category"),
                "event_type": row.get("event_type"),
                "source": row.get("source"),
                "symbol": row.get("symbol"),
                "zone": row.get("zone"),
                "action": row.get("action"),
                "reason": row.get("reason"),
                "order_id": row.get("order_id"),
                "risk_state": row.get("risk_state"),
                "contracts": row.get("contracts"),
                "order_status": row.get("order_status"),
                "guard_reason": row.get("guard_reason"),
                "decision_side": row.get("decision_side"),
                "decision_price": row.get("decision_price"),
                "expected_fill_price": row.get("expected_fill_price"),
            }
        )

    timeline.sort(key=lambda item: ((item.get("timestamp") or ""), 0 if item.get("kind") == "trade" else 1))
    return timeline


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


def _timeline_blockers(timeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for item in timeline:
        if (
            item.get("kind") in {"event", "decision_snapshot", "order_lifecycle"}
            and (
                item.get("event_type") in {
                    "trade_blocked",
                    "risk_state_changed",
                    "fail_safe_activated",
                    "duplicate_unresolved_entry_detected",
                    "unresolved_entry_tracked",
                    "unresolved_entry_cleared",
                }
                or item.get("guard_reason")
                or item.get("reason")
                or item.get("outcome_reason")
            )
        ):
            blockers.append(item)
    return blockers


def _market_tape_analysis(market_tape: list[dict[str, Any]], trade: dict[str, Any]) -> dict[str, Any]:
    prices: list[float] = []
    bid_values: list[float] = []
    ask_values: list[float] = []
    volume_values: list[float] = []

    for row in market_tape:
        last_value = _as_float(row.get("last"))
        bid_value = _as_float(row.get("bid"))
        ask_value = _as_float(row.get("ask"))
        volume_value = _as_float(row.get("volume"))
        if last_value is not None:
            prices.append(last_value)
        if bid_value is not None:
            bid_values.append(bid_value)
        if ask_value is not None:
            ask_values.append(ask_value)
        if volume_value is not None:
            volume_values.append(volume_value)

    entry_price = _as_float(trade.get("entry_price") or trade.get("entryPrice"))
    exit_price = _as_float(trade.get("exit_price") or trade.get("exitPrice"))
    start_price = prices[0] if prices else entry_price
    end_price = prices[-1] if prices else exit_price
    sample_count = len(market_tape)
    minimum = min(prices or bid_values or ask_values or [entry_price or 0.0, exit_price or 0.0])
    maximum = max(prices or bid_values or ask_values or [entry_price or 0.0, exit_price or 0.0])
    spread = None
    if bid_values and ask_values:
        spread = max(ask_values[-1] - bid_values[-1], 0.0)

    return {
        "sampleCount": sample_count,
        "startPrice": start_price,
        "endPrice": end_price,
        "minPrice": minimum,
        "maxPrice": maximum,
        "range": (maximum - minimum) if minimum is not None and maximum is not None else None,
        "spread": spread,
        "priceChange": (end_price - start_price) if start_price is not None and end_price is not None else None,
        "volumeSamples": len(volume_values),
        "latestVolume": volume_values[-1] if volume_values else None,
    }


def _decision_features(decision: dict[str, Any] | None) -> dict[str, Any]:
    if not decision:
        return {}
    feature_snapshot = decision.get("feature_snapshot_json") or decision.get("featureSnapshot") or decision.get("feature_snapshot") or {}
    if not isinstance(feature_snapshot, dict):
        feature_snapshot = {}
    signed_features = feature_snapshot.get("signed_features") or feature_snapshot.get("signedFeatures") or {}
    long_features = feature_snapshot.get("long_features") or feature_snapshot.get("longFeatures") or {}
    short_features = feature_snapshot.get("short_features") or feature_snapshot.get("shortFeatures") or {}
    flat_features = feature_snapshot.get("flat_features") or feature_snapshot.get("flatFeatures") or {}
    diagnostics = feature_snapshot.get("diagnostics") or {}
    capabilities = feature_snapshot.get("capabilities") or {}
    return {
        "featureSnapshot": feature_snapshot,
        "signedFeatures": signed_features if isinstance(signed_features, dict) else {},
        "longFeatures": long_features if isinstance(long_features, dict) else {},
        "shortFeatures": short_features if isinstance(short_features, dict) else {},
        "flatFeatures": flat_features if isinstance(flat_features, dict) else {},
        "diagnostics": diagnostics if isinstance(diagnostics, dict) else {},
        "capabilities": capabilities if isinstance(capabilities, dict) else {},
    }


def _build_trade_review_analysis(
    *,
    trade: dict[str, Any],
    run: dict[str, Any] | None,
    market_tape: list[dict[str, Any]],
    decision_snapshots: list[dict[str, Any]],
    state_snapshots: list[dict[str, Any]],
    order_lifecycle: list[dict[str, Any]],
    events: list[dict[str, Any]],
    timeline: list[dict[str, Any]],
    blockers: list[dict[str, Any]],
) -> dict[str, Any]:
    primary_decision = decision_snapshots[0] if decision_snapshots else None
    trade_decision_id = trade.get("decision_id") or trade.get("decisionId")
    if trade_decision_id:
        matched = next((item for item in decision_snapshots if item.get("decision_id") == trade_decision_id or item.get("decisionId") == trade_decision_id), None)
        if matched:
            primary_decision = matched

    trade_direction = _trade_direction_label(trade.get("direction") or trade.get("Direction"))
    entry_price = _as_float(trade.get("entry_price") or trade.get("entryPrice"))
    exit_price = _as_float(trade.get("exit_price") or trade.get("exitPrice"))
    pnl = _as_float(trade.get("pnl") or trade.get("Pnl"))
    entry_time = _parse_dt(trade.get("entry_time") or trade.get("entryTime"))
    exit_time = _parse_dt(trade.get("exit_time") or trade.get("exitTime"))
    duration = _format_minutes((exit_time - entry_time) if entry_time and exit_time else None)

    features = _decision_features(primary_decision)
    feature_snapshot = features.get("featureSnapshot", {})
    signed_features = features.get("signedFeatures", {})
    diagnostics = features.get("diagnostics", {})
    long_features = features.get("longFeatures", {})
    short_features = features.get("shortFeatures", {})
    flat_features = features.get("flatFeatures", {})

    dominant_side = primary_decision.get("dominant_side") if primary_decision else None
    score_gap = _as_float(primary_decision.get("score_gap")) if primary_decision else None
    long_score = _as_float(primary_decision.get("long_score")) if primary_decision else None
    short_score = _as_float(primary_decision.get("short_score")) if primary_decision else None
    allow_entries = primary_decision.get("allow_entries") if primary_decision else None
    execution_tradeable = primary_decision.get("execution_tradeable") if primary_decision else None
    regime_state = primary_decision.get("regime_state") if primary_decision else None
    regime_reason = primary_decision.get("regime_reason") if primary_decision else None
    active_session = primary_decision.get("active_session") if primary_decision else None

    market_notes: list[str] = []
    if market_tape:
        stats = _market_tape_analysis(market_tape, trade)
        if stats.get("priceChange") is not None:
            market_notes.append(f"Price moved {stats['priceChange']:+.2f} across the sampled trade window.")
        if stats.get("spread") is not None:
            market_notes.append(f"Latest spread in window: {stats['spread']:.2f}.")
        if stats.get("range") is not None:
            market_notes.append(f"Window range: {stats['minPrice']:.2f} to {stats['maxPrice']:.2f}.")
    else:
        market_notes.append("No market tape samples were available for the trade window.")

    execution_notes: list[str] = []
    if dominant_side:
        execution_notes.append(f"Executor dominant side: {dominant_side}.")
    if allow_entries is False:
        execution_notes.append("Entries were blocked by the executor snapshot.")
    elif allow_entries is True:
        execution_notes.append("Entries were allowed by the executor snapshot.")
    if execution_tradeable is False:
        execution_notes.append("Market was marked non-tradeable.")
    elif execution_tradeable is True:
        execution_notes.append("Market was marked tradeable.")
    if regime_state:
        execution_notes.append(f"Regime state: {regime_state}{f' ({regime_reason})' if regime_reason else ''}.")
    if active_session:
        execution_notes.append(f"Active session: {active_session}.")
    if score_gap is not None:
        execution_notes.append(f"Score gap: {score_gap:+.2f}.")

    if feature_snapshot:
        session = feature_snapshot.get("active_session") or active_session
        atr_value = _as_float(feature_snapshot.get("atr_value"))
        rsi_value = _as_float(feature_snapshot.get("rsi"))
        vwap = _as_float(feature_snapshot.get("rth_vwap")) or _as_float(feature_snapshot.get("eth_vwap"))
        spread = _as_float(feature_snapshot.get("spread"))
        volume_profile = feature_snapshot.get("diagnostics", {}).get("volume_profile") if isinstance(feature_snapshot.get("diagnostics"), dict) else None
        if session:
            execution_notes.append(f"Feature session: {session}.")
        if atr_value is not None:
            execution_notes.append(f"ATR: {atr_value:.2f}.")
        if rsi_value is not None:
            execution_notes.append(f"RSI: {rsi_value:.1f}.")
        if vwap is not None:
            execution_notes.append(f"VWAP reference: {vwap:.2f}.")
        if spread is not None:
            execution_notes.append(f"Spread: {spread:.2f}.")
        if volume_profile and isinstance(volume_profile, dict):
            poc = volume_profile.get("poc")
            vah = volume_profile.get("vah")
            val = volume_profile.get("val")
            if poc is not None and vah is not None and val is not None:
                execution_notes.append(f"Volume profile POC/VAH/VAL: {poc} / {vah} / {val}.")

    narrative: list[str] = []
    if primary_decision:
        narrative.append(
            f"The executor read {trade_direction} pressure with long score {long_score if long_score is not None else 'n/a'} and short score {short_score if short_score is not None else 'n/a'}."
        )
        if trade_decision_id:
            narrative.append(f"Trade references decision {trade_decision_id}.")
        if execution_tradeable is False:
            narrative.append("The snapshot said the market was not tradeable, which should be treated as a hard execution warning.")
        elif execution_tradeable is True:
            narrative.append("The snapshot said the market was tradeable.")
        if blockers:
            narrative.append(f"{len(blockers)} blocker events were present in the reconstructed run timeline.")
        if signed_features and isinstance(signed_features, dict):
            top_signed = sorted(
                ((name, _as_float(value)) for name, value in signed_features.items()),
                key=lambda item: abs(item[1] or 0.0),
                reverse=True,
            )[:3]
            compact = ", ".join(f"{name}={value:+.2f}" for name, value in top_signed if value is not None)
            if compact:
                narrative.append(f"Strongest signed features: {compact}.")
    else:
        narrative.append("No matching decision snapshot was found for this trade.")

    market_context = {
        "sampleCount": len(market_tape),
        "firstTimestamp": market_tape[0].get("captured_at") if market_tape else None,
        "lastTimestamp": market_tape[-1].get("captured_at") if market_tape else None,
        "firstPrice": market_tape[0].get("last") if market_tape else None,
        "lastPrice": market_tape[-1].get("last") if market_tape else None,
        "stats": _market_tape_analysis(market_tape, trade),
    }
    entry_exit = {
        "direction": trade_direction,
        "entryTime": _iso(entry_time),
        "exitTime": _iso(exit_time),
        "duration": duration,
        "entryPrice": entry_price,
        "exitPrice": exit_price,
        "pnl": pnl,
    }

    executor_projection = [
        note
        for note in [
            f"Dominant side leans {dominant_side}." if dominant_side else None,
            "Entries are blocked by snapshot state." if allow_entries is False else None,
            "Market tradeable flag is set." if execution_tradeable is True else ("Market marked non-tradeable." if execution_tradeable is False else None),
            f"Regime context: {regime_state}." if regime_state else None,
            f"ATR {_as_float(feature_snapshot.get('atr_value')):.2f}" if _as_float(feature_snapshot.get("atr_value")) is not None else None,
        ]
        if note
    ]

    return {
        "summary": f"{trade_direction.title()} trade on {run.get('symbol') if run else 'ES'}: entry {entry_price if entry_price is not None else 'n/a'}, exit {exit_price if exit_price is not None else 'n/a'}, pnl {pnl if pnl is not None else 'n/a'}.",
        "marketContext": market_context,
        "entryExit": entry_exit,
        "executorBelief": {
            "decisionId": trade_decision_id,
            "dominantSide": dominant_side,
            "scoreGap": score_gap,
            "longScore": long_score,
            "shortScore": short_score,
            "allowEntries": allow_entries,
            "executionTradeable": execution_tradeable,
            "regimeState": regime_state,
            "regimeReason": regime_reason,
            "activeSession": active_session,
            "featureSnapshot": feature_snapshot,
            "signedFeatures": signed_features,
            "diagnostics": diagnostics,
            "longFeatures": long_features,
            "shortFeatures": short_features,
            "flatFeatures": flat_features,
        },
        "narrative": narrative,
        "marketNotes": market_notes,
        "executionNotes": execution_notes,
        "executorProjection": executor_projection,
    }


@app.get("/trades/{trade_id}")
async def trade_review(
    trade_id: int,
    authorization: str | None = Header(None),
    limit: int = Query(200, ge=1, le=1000),
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
               WHERE id = %s""",
            (trade_id,),
        )
        trade = cur.fetchone()
        if not trade:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Trade not found")

        trade_dict = dict(trade)
        run_id = trade_dict.get("run_id")
        cur.execute(
            """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol, status, zone, zone_state,
                      position, position_pnl, daily_pnl, risk_state, account_id, account_name, account_mode,
                      account_is_practice, last_signal_json, last_entry_block_reason, execution_json, heartbeat_json,
                      lifecycle_json, payload_json
               FROM runs
               WHERE run_id = %s""",
            (run_id,),
        )
        run = cur.fetchone()
        if not run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found for trade")
        run_dict = dict(run)

        entry_time = _parse_dt(trade_dict.get("entry_time") or trade_dict.get("inserted_at"))
        exit_time = _parse_dt(trade_dict.get("exit_time") or trade_dict.get("entry_time") or trade_dict.get("inserted_at"))
        market_limit = min(max(limit, 100), 1000)
        window_start, window_end = _trade_window_bounds(trade_dict, padding_minutes=10)
        if entry_time and exit_time:
            market_start = entry_time - timedelta(minutes=10)
            market_end = exit_time + timedelta(minutes=10)
            cur.execute(
                """SELECT id, run_id, captured_at, inserted_at, process_id, symbol, contract_id, bid, ask, last, volume,
                          bid_size, ask_size, last_size, volume_is_cumulative, quote_is_synthetic, trade_side, latency_ms,
                          source, sequence, payload_json
                   FROM market_tape
                   WHERE run_id = %s
                     AND captured_at BETWEEN %s AND %s
                   ORDER BY captured_at ASC, id ASC
                   LIMIT %s""",
                (run_id, market_start, market_end, market_limit),
            )
        else:
            cur.execute(
                """SELECT id, run_id, captured_at, inserted_at, process_id, symbol, contract_id, bid, ask, last, volume,
                          bid_size, ask_size, last_size, volume_is_cumulative, quote_is_synthetic, trade_side, latency_ms,
                          source, sequence, payload_json
                   FROM market_tape
                   WHERE run_id = %s
                   ORDER BY captured_at DESC, id DESC
                   LIMIT %s""",
                (run_id, market_limit),
            )
        market_tape = [dict(row) for row in cur.fetchall()]

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
            (run_id, market_limit),
        )
        decision_snapshots = [dict(row) for row in cur.fetchall()]

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
            (run_id, market_limit),
        )
        state_snapshots = [dict(row) for row in cur.fetchall()]

        lifecycle_related: list[str] = []
        lifecycle_params: list[Any] = [run_id]
        for key in ("trade_id", "decision_id", "position_id"):
            value = trade_dict.get(key)
            if value:
                lifecycle_related.append(f"{key} = %s")
                lifecycle_params.append(value)
        lifecycle_params.append(market_limit)
        lifecycle_where = "run_id = %s"
        if lifecycle_related:
            lifecycle_where = f"run_id = %s AND ({' OR '.join(lifecycle_related)})"
        cur.execute(
            f"""SELECT id, run_id, observed_at, inserted_at, decision_id, attempt_id, order_id, position_id, trade_id,
                      process_id, symbol, event_type, status, side, role, is_protective, order_type, quantity, contracts,
                      limit_price, stop_price, expected_fill_price, filled_price, filled_quantity, remaining_quantity,
                      zone, reason, lifecycle_state, payload_json
               FROM order_lifecycle
               WHERE {lifecycle_where}
               ORDER BY observed_at DESC, id DESC
               LIMIT %s""",
            tuple(lifecycle_params),
        )
        order_lifecycle = [dict(row) for row in cur.fetchall()]
        order_ids = [row.get("order_id") for row in order_lifecycle if row.get("order_id")]

        event_clauses = ["run_id = %s"]
        event_params: list[Any] = [run_id]
        if entry_time and exit_time:
            event_clauses.append("event_timestamp BETWEEN %s AND %s")
            event_params.extend([entry_time - timedelta(minutes=10), exit_time + timedelta(minutes=10)])
        if order_ids:
            event_clauses.append("order_id = ANY(%s)")
            event_params.append(order_ids)
        event_params.append(market_limit)
        cur.execute(
            f"""SELECT id, run_id, event_timestamp, inserted_at, category, event_type, source, symbol, zone, action,
                       reason, order_id, risk_state, contracts, order_status, guard_reason, decision_side, decision_price,
                       expected_fill_price, entry_guard_json, unresolved_entry_json, execution_json, payload_json
                FROM events
                WHERE {' AND '.join(event_clauses)}
                ORDER BY event_timestamp DESC, id DESC
                LIMIT %s""",
            tuple(event_params),
        )
        events = [dict(row) for row in cur.fetchall()]

        cur.execute(
            """SELECT *
               FROM v_run_timeline
               WHERE run_id = %s
               ORDER BY timeline_at DESC
               LIMIT %s""",
            (run_id, market_limit),
        )
        timeline_rows = [dict(row) for row in cur.fetchall()]
        timeline: list[dict[str, Any]] = []
        for row in timeline_rows:
            item = dict(row)
            item["timestamp"] = _iso(item.get("timeline_at"))
            timeline.append(item)
        blockers = _timeline_blockers(timeline)

        analysis = _build_trade_review_analysis(
            trade=trade_dict,
            run=run_dict,
            market_tape=market_tape,
            decision_snapshots=decision_snapshots,
            state_snapshots=state_snapshots,
            order_lifecycle=order_lifecycle,
            events=events,
            timeline=timeline,
            blockers=blockers,
        )

        return {
            "trade": trade_dict,
            "run": run_dict,
            "account": {
                "account_id": trade_dict.get("account_id") or run_dict.get("account_id"),
                "account_name": trade_dict.get("account_name") or run_dict.get("account_name"),
                "account_mode": trade_dict.get("account_mode") or run_dict.get("account_mode"),
                "account_is_practice": trade_dict.get("account_is_practice") if trade_dict.get("account_is_practice") is not None else run_dict.get("account_is_practice"),
            },
            "window": {
                "start": _iso(window_start),
                "end": _iso(window_end),
                "entry_time": _iso(trade_dict.get("entry_time")),
                "exit_time": _iso(trade_dict.get("exit_time")),
                "padding_minutes": 10 if entry_time and exit_time else 0,
            },
            "entry": {
                "timestamp": _iso(trade_dict.get("entry_time")),
                "price": trade_dict.get("entry_price"),
                "direction": _trade_direction_label(trade_dict.get("direction")),
                "contracts": trade_dict.get("contracts"),
                "zone": trade_dict.get("zone"),
                "strategy": trade_dict.get("strategy"),
                "trade_id": trade_dict.get("trade_id") or trade_dict.get("id"),
            },
            "exit": {
                "timestamp": _iso(trade_dict.get("exit_time")),
                "price": trade_dict.get("exit_price"),
                "direction": _trade_direction_label(trade_dict.get("direction")),
                "contracts": trade_dict.get("contracts"),
                "zone": trade_dict.get("zone"),
                "strategy": trade_dict.get("strategy"),
                "trade_id": trade_dict.get("trade_id") or trade_dict.get("id"),
            },
            "marketTape": market_tape,
            "marketContext": analysis["marketContext"],
            "entryExit": analysis["entryExit"],
            "decisionSnapshots": decision_snapshots,
            "stateSnapshots": state_snapshots,
            "orderLifecycle": order_lifecycle,
            "events": events,
            "timeline": timeline,
            "blockers": blockers,
            "analysis": analysis,
        }
    finally:
        put_conn(conn)


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
