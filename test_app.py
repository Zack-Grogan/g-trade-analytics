from __future__ import annotations

import asyncio
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

    def cursor(self, cursor_factory=None):  # noqa: ANN001 - matches psycopg2 API
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True


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
