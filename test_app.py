from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys

from fastapi.testclient import TestClient


APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import app as analytics_app
import graphql_resolvers as resolvers


class _FakeCursor:
    def __init__(
        self,
        rows: list[dict] | None = None,
        row: dict | None = None,
        row_sequence: list[dict] | None = None,
    ):
        self.rows = rows or []
        self.row = row
        self.row_sequence = list(row_sequence or [])
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, query: str, params: tuple | None = None) -> None:
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        if self.row_sequence:
            return self.row_sequence.pop(0)
        return self.row

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(
        self,
        rows: list[dict] | None = None,
        row: dict | None = None,
        row_sequence: list[dict] | None = None,
    ):
        self.cursor_obj = _FakeCursor(rows=rows, row=row, row_sequence=row_sequence)
        self.committed = False
        self.rolled_back = False

    def cursor(self, cursor_factory=None):  # noqa: ANN001 - matches psycopg2 API
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class _SequencedFakeCursor:
    def __init__(self, responses: list[tuple[str, dict]]):
        self.responses = list(responses)
        self.executed: list[tuple[str, tuple | None]] = []
        self.rows: list[dict] = []
        self.row: dict | None = None

    def execute(self, query: str, params: tuple | None = None) -> None:
        self.executed.append((query, params))
        if not self.responses:
            raise AssertionError(f"Unexpected query: {query}")
        expected, payload = self.responses.pop(0)
        assert expected in query, f"Expected {expected!r} in query but saw {query!r}"
        self.rows = payload.get("rows", [])
        self.row = payload.get("row")

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.row

    def close(self) -> None:
        pass


class _SequencedFakeConn:
    def __init__(self, responses: list[tuple[str, dict]]):
        self.cursor_obj = _SequencedFakeCursor(responses)
        self.committed = False
        self.rolled_back = False

    def cursor(self, cursor_factory=None):  # noqa: ANN001 - matches psycopg2 API
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class _FakePool:
    minconn = 2
    maxconn = 10
    closed = False
    _pool = [object(), object(), object()]
    _used = {1: object()}


def test_resolve_runs_returns_connection_to_pool(monkeypatch):
    fake_conn = _FakeConn(
        rows=[
            {
                "run_id": "run-1",
                "created_at": "2026-03-18T00:00:00+00:00",
                "process_id": 123,
                "data_mode": "live",
                "symbol": "ES",
                "payload_json": {"run_id": "run-1"},
            }
        ]
    )
    returned: list[_FakeConn] = []

    monkeypatch.setattr(resolvers, "_auth", lambda info: None)
    monkeypatch.setattr(resolvers, "get_conn", lambda: fake_conn)
    monkeypatch.setattr(resolvers, "put_conn", lambda conn: returned.append(conn))

    rows = asyncio.run(resolvers.resolve_runs(object()))

    assert len(rows) == 1
    assert rows[0].run_id == "run-1"
    assert returned == [fake_conn]


def test_health_reports_pool_state(monkeypatch):
    monkeypatch.setattr(analytics_app, "ensure_schema_v2", lambda: None)
    monkeypatch.setattr(analytics_app, "ANALYTICS_API_KEY", "test-token")
    monkeypatch.setattr(analytics_app, "_pool", _FakePool())

    with TestClient(analytics_app.app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "pool": {
            "pool_minconn": 2,
            "pool_maxconn": 10,
            "pool_available": 3,
            "pool_borrowed": 1,
            "pool_closed": False,
        },
    }


def test_graphql_requires_auth(monkeypatch):
    monkeypatch.setattr(analytics_app, "ensure_schema_v2", lambda: None)
    monkeypatch.setattr(analytics_app, "ANALYTICS_API_KEY", "test-token")
    monkeypatch.setattr(analytics_app, "_pool", _FakePool())

    with TestClient(analytics_app.app) as client:
        unauthorized = client.get("/graphql")
        authorized = client.get("/graphql", headers={"Authorization": "Bearer test-token"})

    assert unauthorized.status_code == 401
    assert authorized.status_code == 200
    assert "Strawberry GraphiQL" in authorized.text


def test_graphql_is_query_only(monkeypatch):
    monkeypatch.setattr(analytics_app, "ensure_schema_v2", lambda: None)
    monkeypatch.setattr(analytics_app, "ANALYTICS_API_KEY", "test-token")
    monkeypatch.setattr(analytics_app, "_pool", _FakePool())

    with TestClient(analytics_app.app) as client:
        response = client.post(
            "/graphql",
            headers={"Authorization": "Bearer test-token"},
            json={
                "query": """
                    mutation {
                      generateHypothesis(context: { generation: 1 }) {
                        hypothesisId
                      }
                    }
                """
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["data"] is None
    assert body["errors"]


def test_search_runtime_logs_reads_runtime_log_rows(monkeypatch):
    fake_conn = _FakeConn(
        rows=[
            {
                "id": 1,
                "run_id": "run-1",
                "logged_at": "2026-03-18T10:00:00+00:00",
                "inserted_at": "2026-03-18T10:00:01+00:00",
                "level": "ERROR",
                "logger_name": "bridge",
                "source": "railway-bridge",
                "service_name": "es-hotzone-trader",
                "process_id": 42,
                "line_hash": "abc",
                "message": "HTTP 401",
                "payload_json": {"last_error": "HTTP 401"},
            }
        ]
    )

    monkeypatch.setattr(analytics_app, "ensure_schema_v2", lambda: None)
    monkeypatch.setattr(analytics_app, "ANALYTICS_API_KEY", "test-token")
    monkeypatch.setattr(analytics_app, "_pool", _FakePool())
    monkeypatch.setattr(analytics_app, "get_conn", lambda: fake_conn)
    monkeypatch.setattr(analytics_app, "put_conn", lambda conn: None)

    with TestClient(analytics_app.app) as client:
        response = client.get(
            "/search/runtime_logs",
            headers={"Authorization": "Bearer test-token"},
            params={"q": "401"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["runtimeLogs"][0]["message"] == "HTTP 401"


def test_trade_review_requires_auth(monkeypatch):
    monkeypatch.setattr(analytics_app, "ensure_schema_v2", lambda: None)
    monkeypatch.setattr(analytics_app, "ANALYTICS_API_KEY", "test-token")
    monkeypatch.setattr(analytics_app, "_pool", _FakePool())

    with TestClient(analytics_app.app) as client:
        response = client.get("/trades/42")

    assert response.status_code == 401


def test_trade_review_returns_404_for_missing_trade(monkeypatch):
    fake_conn = _SequencedFakeConn(
        [
            (
                "FROM completed_trades",
                {"row": None},
            ),
            (
                "FROM account_trades",
                {"row": None},
            ),
        ]
    )

    monkeypatch.setattr(analytics_app, "ensure_schema_v2", lambda *args, **kwargs: None)
    monkeypatch.setattr(analytics_app, "ANALYTICS_API_KEY", "test-token")
    monkeypatch.setattr(analytics_app, "_pool", _FakePool())
    monkeypatch.setattr(analytics_app, "get_conn", lambda: fake_conn)
    monkeypatch.setattr(analytics_app, "put_conn", lambda conn: None)

    with TestClient(analytics_app.app) as client:
        response = client.get("/trades/42", headers={"Authorization": "Bearer test-token"})

    assert response.status_code == 404
    assert response.json()["detail"] == "Trade not found"


def test_trade_review_resolves_account_trade_identifier(monkeypatch):
    fake_conn = _SequencedFakeConn(
        [
            (
                "FROM completed_trades",
                {"row": None},
            ),
            (
                "FROM account_trades",
                {
                    "row": {
                        "id": 77,
                        "run_id": None,
                        "inserted_at": None,
                        "occurred_at": datetime(2026, 3, 18, 10, 25, tzinfo=timezone.utc),
                        "account_id": "acct-1",
                        "account_name": "Practice",
                        "account_mode": "practice",
                        "account_is_practice": True,
                        "broker_trade_id": "2299253381",
                        "broker_order_id": "order-77",
                        "contract_id": "ESM6",
                        "side": 1,
                        "size": 1,
                        "price": 4512.75,
                        "profit_and_loss": 243.5,
                        "fees": 2.5,
                        "voided": False,
                        "source": "broker",
                        "payload_json": {"broker_trade_id": "2299253381"},
                    }
                },
            ),
        ]
    )

    monkeypatch.setattr(analytics_app, "ensure_schema_v2", lambda *args, **kwargs: None)
    monkeypatch.setattr(analytics_app, "ANALYTICS_API_KEY", "test-token")
    monkeypatch.setattr(analytics_app, "_pool", _FakePool())
    monkeypatch.setattr(analytics_app, "get_conn", lambda: fake_conn)
    monkeypatch.setattr(analytics_app, "put_conn", lambda conn: None)

    with TestClient(analytics_app.app) as client:
        response = client.get("/trades/2299253381", headers={"Authorization": "Bearer test-token"})

    assert response.status_code == 200
    body = response.json()
    assert body["trade"]["trade_id"] == "2299253381"
    assert body["trade"]["account_id"] == "acct-1"
    assert body["run"]["run_id"] == "account-trade-2299253381"
    assert body["account"]["account_mode"] == "practice"
    assert body["analysis"]["entryExit"]["direction"] == "long"


def test_trade_review_returns_normalized_bundle(monkeypatch):
    entry_time = datetime(2026, 3, 18, 10, 5, tzinfo=timezone.utc)
    exit_time = datetime(2026, 3, 18, 10, 25, tzinfo=timezone.utc)
    window_start = datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)
    window_end = datetime(2026, 3, 18, 10, 30, tzinfo=timezone.utc)

    fake_conn = _SequencedFakeConn(
        [
            (
                "FROM completed_trades",
                {
                    "row": {
                        "id": 42,
                        "run_id": "run-1",
                        "inserted_at": entry_time,
                        "entry_time": entry_time,
                        "exit_time": exit_time,
                        "direction": 1,
                        "contracts": 2,
                        "entry_price": 4500.5,
                        "exit_price": 4512.75,
                        "pnl": 243.5,
                        "zone": "zone-a",
                        "strategy": "hot-zone",
                        "regime": "trend",
                        "event_tags_json": ["entry", "exit"],
                        "source": "broker",
                        "backfilled": True,
                        "trade_id": "trade-42",
                        "position_id": "position-42",
                        "decision_id": "decision-1",
                        "attempt_id": "attempt-1",
                        "account_id": "acct-1",
                        "account_name": "Practice",
                        "account_mode": "practice",
                        "account_is_practice": True,
                        "payload_json": {"trade_id": "trade-42"},
                    }
                },
            ),
            (
                "FROM runs",
                {
                    "row": {
                        "run_id": "run-1",
                        "created_at": window_start,
                        "last_seen_at": window_end,
                        "process_id": 7,
                        "data_mode": "live",
                        "symbol": "ES",
                        "status": "active",
                        "zone": "zone-a",
                        "zone_state": "entering",
                        "position": 0,
                        "position_pnl": 123.4,
                        "daily_pnl": 432.1,
                        "risk_state": "watching",
                        "account_id": "acct-1",
                        "account_name": "Practice",
                        "account_mode": "practice",
                        "account_is_practice": True,
                        "last_signal_json": {"signal": "long"},
                        "last_entry_block_reason": None,
                        "execution_json": {"executor": "advisory"},
                        "heartbeat_json": {"alive": True},
                        "lifecycle_json": {"stage": "live"},
                        "payload_json": {"run_id": "run-1"},
                    }
                },
            ),
            (
                "FROM market_tape",
                {
                    "rows": [
                        {
                            "id": 100,
                            "run_id": "run-1",
                            "captured_at": datetime(2026, 3, 18, 10, 5, 30, tzinfo=timezone.utc),
                            "inserted_at": datetime(2026, 3, 18, 10, 5, 31, tzinfo=timezone.utc),
                            "process_id": 7,
                            "symbol": "ES",
                            "contract_id": "ESM6",
                            "bid": 4500.25,
                            "ask": 4500.75,
                            "last": 4500.5,
                            "volume": 10,
                            "bid_size": 3,
                            "ask_size": 4,
                            "last_size": 2,
                            "volume_is_cumulative": False,
                            "quote_is_synthetic": False,
                            "trade_side": "buy",
                            "latency_ms": 12,
                            "source": "feed",
                            "sequence": 1,
                            "payload_json": {"last": 4500.5},
                        },
                        {
                            "id": 101,
                            "run_id": "run-1",
                            "captured_at": datetime(2026, 3, 18, 10, 24, 30, tzinfo=timezone.utc),
                            "inserted_at": datetime(2026, 3, 18, 10, 24, 31, tzinfo=timezone.utc),
                            "process_id": 7,
                            "symbol": "ES",
                            "contract_id": "ESM6",
                            "bid": 4512.5,
                            "ask": 4513.0,
                            "last": 4512.75,
                            "volume": 18,
                            "bid_size": 2,
                            "ask_size": 5,
                            "last_size": 1,
                            "volume_is_cumulative": False,
                            "quote_is_synthetic": False,
                            "trade_side": "buy",
                            "latency_ms": 9,
                            "source": "feed",
                            "sequence": 2,
                            "payload_json": {"last": 4512.75},
                        },
                    ]
                },
            ),
            (
                "FROM decision_snapshots",
                {
                    "rows": [
                        {
                            "id": 1,
                            "run_id": "run-1",
                            "decided_at": datetime(2026, 3, 18, 10, 4, 50, tzinfo=timezone.utc),
                            "inserted_at": datetime(2026, 3, 18, 10, 4, 51, tzinfo=timezone.utc),
                            "decision_id": "decision-1",
                            "attempt_id": "attempt-1",
                            "process_id": 7,
                            "symbol": "ES",
                            "zone": "zone-a",
                            "action": "enter_long",
                            "reason": "setup approved",
                            "outcome": "order_submitted",
                            "outcome_reason": "executor approved",
                            "long_score": 0.83,
                            "short_score": 0.11,
                            "flat_bias": 0.06,
                            "score_gap": 0.72,
                            "dominant_side": "long",
                            "current_price": 4500.5,
                            "allow_entries": True,
                            "execution_tradeable": True,
                            "contracts": 2,
                            "order_type": "limit",
                            "limit_price": 4500.5,
                            "decision_price": 4500.5,
                            "side": "long",
                            "stop_loss": 4498.5,
                            "take_profit": 4512.75,
                            "max_hold_minutes": 20,
                            "regime_state": "trend",
                            "regime_reason": "strong trend",
                            "active_session": "rth",
                            "active_vetoes_json": [],
                            "feature_snapshot_json": {"atr": 1.25, "rsi": 58.2},
                            "entry_guard_json": {"allow_entries": True},
                            "unresolved_entry_json": None,
                            "event_context_json": {"market": "calm"},
                            "order_flow_json": {"flow": "entry"},
                            "payload_json": {"decision_id": "decision-1"},
                        }
                    ]
                },
            ),
            (
                "FROM state_snapshots",
                {
                    "rows": [
                        {
                            "id": 1,
                            "run_id": "run-1",
                            "captured_at": datetime(2026, 3, 18, 10, 4, 30, tzinfo=timezone.utc),
                            "status": "watching",
                            "data_mode": "live",
                            "symbol": "ES",
                            "zone": "zone-a",
                            "zone_state": "watching",
                            "position": 0,
                            "position_pnl": 0.0,
                            "daily_pnl": 120.0,
                            "risk_state": "watching",
                            "account_id": "acct-1",
                            "account_name": "Practice",
                            "account_mode": "practice",
                            "account_is_practice": True,
                            "last_signal_json": {"signal": "flat"},
                            "last_entry_reason": "setup approved",
                            "last_entry_block_reason": None,
                            "decision_price": 4500.5,
                            "entry_guard_json": {"allow_entries": True},
                            "unresolved_entry_json": None,
                            "execution_json": {"executor": "advisory"},
                            "heartbeat_json": {"alive": True},
                            "lifecycle_json": {"stage": "watching"},
                            "observability_json": {"source": "local"},
                            "payload_json": {"captured_at": "before"},
                        },
                        {
                            "id": 2,
                            "run_id": "run-1",
                            "captured_at": datetime(2026, 3, 18, 10, 25, 30, tzinfo=timezone.utc),
                            "status": "flat",
                            "data_mode": "live",
                            "symbol": "ES",
                            "zone": "zone-a",
                            "zone_state": "flat",
                            "position": 0,
                            "position_pnl": 243.5,
                            "daily_pnl": 363.5,
                            "risk_state": "flat",
                            "account_id": "acct-1",
                            "account_name": "Practice",
                            "account_mode": "practice",
                            "account_is_practice": True,
                            "last_signal_json": {"signal": "exit"},
                            "last_entry_reason": "trade complete",
                            "last_entry_block_reason": None,
                            "decision_price": 4512.75,
                            "entry_guard_json": {"allow_entries": True},
                            "unresolved_entry_json": None,
                            "execution_json": {"executor": "advisory"},
                            "heartbeat_json": {"alive": True},
                            "lifecycle_json": {"stage": "flat"},
                            "observability_json": {"source": "local"},
                            "payload_json": {"captured_at": "after"},
                        },
                    ]
                },
            ),
            (
                "FROM order_lifecycle",
                {
                    "rows": [
                        {
                            "id": 1,
                            "run_id": "run-1",
                            "observed_at": datetime(2026, 3, 18, 10, 6, tzinfo=timezone.utc),
                            "inserted_at": datetime(2026, 3, 18, 10, 6, 1, tzinfo=timezone.utc),
                            "decision_id": "decision-1",
                            "attempt_id": "attempt-1",
                            "order_id": "ord-1",
                            "position_id": "position-42",
                            "trade_id": "trade-42",
                            "process_id": 7,
                            "symbol": "ES",
                            "event_type": "submitted",
                            "status": "filled",
                            "side": "buy",
                            "role": "entry",
                            "is_protective": False,
                            "order_type": "limit",
                            "quantity": 2,
                            "contracts": 2,
                            "limit_price": 4500.5,
                            "stop_price": 4498.5,
                            "expected_fill_price": 4500.75,
                            "filled_price": 4500.5,
                            "filled_quantity": 2,
                            "remaining_quantity": 0,
                            "zone": "zone-a",
                            "reason": "entry approved",
                            "lifecycle_state": "filled",
                            "payload_json": {"order_id": "ord-1"},
                        }
                    ]
                },
            ),
            (
                "FROM events",
                {
                    "rows": [
                        {
                            "id": 9,
                            "run_id": "run-1",
                            "event_timestamp": datetime(2026, 3, 18, 10, 6, 30, tzinfo=timezone.utc),
                            "inserted_at": datetime(2026, 3, 18, 10, 6, 31, tzinfo=timezone.utc),
                            "category": "execution",
                            "event_type": "trade_submitted",
                            "source": "engine",
                            "symbol": "ES",
                            "zone": "zone-a",
                            "action": "submit",
                            "reason": "entry approved",
                            "order_id": "ord-1",
                            "risk_state": "watching",
                            "contracts": 2,
                            "order_status": "filled",
                            "guard_reason": None,
                            "decision_side": "long",
                            "decision_price": 4500.5,
                            "expected_fill_price": 4500.75,
                            "entry_guard_json": {"allow_entries": True},
                            "unresolved_entry_json": None,
                            "execution_json": {"order_id": "ord-1"},
                            "payload_json": {"event": "trade_submitted"},
                        }
                    ]
                },
            ),
            (
                "FROM v_run_timeline",
                {
                    "rows": [
                        {
                            "kind": "decision_snapshot",
                            "timeline_at": datetime(2026, 3, 18, 10, 4, 50, tzinfo=timezone.utc),
                            "event_type": "order_submitted",
                            "reason": "setup approved",
                            "guard_reason": None,
                            "outcome_reason": "executor approved",
                        },
                        {
                            "kind": "order_lifecycle",
                            "timeline_at": datetime(2026, 3, 18, 10, 6, tzinfo=timezone.utc),
                            "event_type": "filled",
                            "reason": "entry approved",
                            "guard_reason": None,
                            "outcome_reason": None,
                        },
                        {
                            "kind": "event",
                            "timeline_at": datetime(2026, 3, 18, 10, 6, 30, tzinfo=timezone.utc),
                            "event_type": "trade_submitted",
                            "reason": "entry approved",
                            "guard_reason": None,
                            "outcome_reason": None,
                        },
                    ]
                },
            ),
        ]
    )

    monkeypatch.setattr(analytics_app, "ensure_schema_v2", lambda *args, **kwargs: None)
    monkeypatch.setattr(analytics_app, "ANALYTICS_API_KEY", "test-token")
    monkeypatch.setattr(analytics_app, "_pool", _FakePool())
    monkeypatch.setattr(analytics_app, "get_conn", lambda: fake_conn)
    monkeypatch.setattr(analytics_app, "put_conn", lambda conn: None)

    with TestClient(analytics_app.app) as client:
        response = client.get("/trades/42", headers={"Authorization": "Bearer test-token"})

    assert response.status_code == 200
    body = response.json()
    assert body["trade"]["id"] == 42
    assert body["run"]["run_id"] == "run-1"
    assert body["account"]["account_mode"] == "practice"
    assert body["window"]["padding_minutes"] == 10
    assert body["entry"]["price"] == 4500.5
    assert body["exit"]["price"] == 4512.75
    assert body["marketContext"]["sampleCount"] == 2
    assert body["analysis"]["summary"].startswith("Long trade on ES:")
    assert body["analysis"]["entryExit"]["direction"] == "long"
    assert body["analysis"]["executorBelief"]["decisionId"] == "decision-1"
    assert body["analysis"]["executorProjection"]
    assert len(body["timeline"]) == 3


def test_ensure_schema_v2_rebuilds_missing_derived_relations(monkeypatch, tmp_path):
    schema_path = tmp_path / "schema_v2.sql"
    schema_path.write_text("SELECT 1;")
    fake_conn = _FakeConn()

    def fake_execute(query: str, params: tuple | None = None) -> None:
        fake_conn.cursor_obj.executed.append((query, params))
        if "FROM pg_class" in query:
            fake_conn.cursor_obj.rows = [
                {"relname": "runs"},
                {"relname": "events"},
                {"relname": "state_snapshots"},
                {"relname": "market_tape"},
                {"relname": "decision_snapshots"},
                {"relname": "order_lifecycle"},
                {"relname": "completed_trades"},
            ]
        elif "FROM pg_attribute" in query:
            fake_conn.cursor_obj.rows = []
        else:
            fake_conn.cursor_obj.rows = []

    fake_conn.cursor_obj.execute = fake_execute

    monkeypatch.setattr(analytics_app, "get_conn", lambda: fake_conn)
    monkeypatch.setattr(analytics_app, "put_conn", lambda conn: None)
    monkeypatch.setattr(analytics_app.os.path, "dirname", lambda _: str(tmp_path))

    assert analytics_app.ensure_schema_v2() is True
    assert fake_conn.committed is True
    executed_sql = "\n".join(query for query, _ in fake_conn.cursor_obj.executed)
    assert "DROP VIEW IF EXISTS v_run_timeline" in executed_sql
    assert "DROP MATERIALIZED VIEW IF EXISTS mv_run_summaries" in executed_sql
    assert "SELECT 1;" in executed_sql


def test_ensure_schema_v2_defers_until_base_tables_exist(monkeypatch, tmp_path):
    schema_path = tmp_path / "schema_v2.sql"
    schema_path.write_text("SELECT 1;")
    fake_conn = _FakeConn()

    def fake_execute(query: str, params: tuple | None = None) -> None:
        fake_conn.cursor_obj.executed.append((query, params))
        if "FROM pg_class" in query:
            fake_conn.cursor_obj.rows = [
                {"relname": "runs"},
                {"relname": "events"},
            ]
        else:
            fake_conn.cursor_obj.rows = []

    fake_conn.cursor_obj.execute = fake_execute

    monkeypatch.setattr(analytics_app, "get_conn", lambda: fake_conn)
    monkeypatch.setattr(analytics_app, "put_conn", lambda conn: None)
    monkeypatch.setattr(analytics_app.os.path, "dirname", lambda _: str(tmp_path))

    assert analytics_app.ensure_schema_v2() is False
    assert fake_conn.committed is False
    executed_sql = "\n".join(query for query, _ in fake_conn.cursor_obj.executed)
    assert "DROP VIEW IF EXISTS v_run_timeline" not in executed_sql
    assert "SELECT 1;" not in executed_sql
