"""
GraphQL resolvers for g-trade-analytics. Query-only read surface over Postgres.
"""
from __future__ import annotations

import os
from typing import Optional

from strawberry.types import Info

import httpx

from graphql_schema import AIReport, Hypothesis, KnowledgeEntry, MetaLearnerStats, Run, Trade
from app import get_conn, put_conn, _require_auth
from psycopg2.extras import RealDictCursor

# Token to authenticate calls from analytics to the RLM service (if deployed with auth).
RLM_AUTH_TOKEN = os.environ.get("RLM_AUTH_TOKEN", "").strip()


def _auth(info: Info) -> None:
    request = info.context.get("request")
    auth = request.headers.get("Authorization") if request else None
    _require_auth(auth)


def _db(info: Info):
    return get_conn()


async def resolve_runs(info: Info, limit: int = 25) -> list[Run]:
    _auth(info)
    conn = _db(info)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol,
                      account_id, account_name, account_mode, account_is_practice, payload_json
               FROM runs
               ORDER BY created_at DESC
               LIMIT %s""",
            (limit,),
        )
        rows = cur.fetchall()
        return [
            Run(
                run_id=r["run_id"],
                created_at=str(r["created_at"]) if r.get("created_at") else "",
                last_seen_at=str(r["last_seen_at"]) if r.get("last_seen_at") else None,
                process_id=r.get("process_id"),
                data_mode=r.get("data_mode"),
                symbol=r.get("symbol"),
                account_id=r.get("account_id"),
                account_name=r.get("account_name"),
                account_mode=r.get("account_mode"),
                account_is_practice=r.get("account_is_practice"),
                payload_json=r.get("payload_json"),
            )
            for r in rows
        ]
    finally:
        put_conn(conn)


async def resolve_run(info: Info, id: str) -> Optional[Run]:
    _auth(info)
    conn = _db(info)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT run_id, created_at, last_seen_at, process_id, data_mode, symbol,
                      account_id, account_name, account_mode, account_is_practice, payload_json
               FROM runs
               WHERE run_id = %s""",
            (id,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return Run(
            run_id=r["run_id"],
            created_at=str(r["created_at"]) if r.get("created_at") else "",
            last_seen_at=str(r["last_seen_at"]) if r.get("last_seen_at") else None,
            process_id=r.get("process_id"),
            data_mode=r.get("data_mode"),
            symbol=r.get("symbol"),
            account_id=r.get("account_id"),
            account_name=r.get("account_name"),
            account_mode=r.get("account_mode"),
            account_is_practice=r.get("account_is_practice"),
            payload_json=r.get("payload_json"),
        )
    finally:
        put_conn(conn)


async def resolve_trades(
    info: Info,
    run_id: Optional[str] = None,
    limit: int = 100,
) -> list[Trade]:
    _auth(info)
    conn = _db(info)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if run_id:
            cur.execute(
                """SELECT id, run_id, inserted_at, entry_time, exit_time, direction, contracts, entry_price, exit_price, pnl,
                          zone, strategy, regime, source, backfilled, trade_id, position_id, decision_id, attempt_id,
                          account_id, account_name, account_mode, account_is_practice, payload_json
                   FROM completed_trades WHERE run_id = %s ORDER BY exit_time DESC LIMIT %s""",
                (run_id, limit),
            )
        else:
            cur.execute(
                """SELECT id, run_id, inserted_at, entry_time, exit_time, direction, contracts, entry_price, exit_price, pnl,
                          zone, strategy, regime, source, backfilled, trade_id, position_id, decision_id, attempt_id,
                          account_id, account_name, account_mode, account_is_practice, payload_json
                   FROM completed_trades ORDER BY exit_time DESC LIMIT %s""",
                (limit,),
            )
        rows = cur.fetchall()
        return [
                Trade(
                    id=r["id"],
                    run_id=r["run_id"],
                    inserted_at=str(r["inserted_at"]) if r.get("inserted_at") else None,
                    entry_time=str(r["entry_time"]) if r.get("entry_time") else None,
                    exit_time=str(r["exit_time"]) if r.get("exit_time") else None,
                direction=r["direction"],
                contracts=r["contracts"],
                entry_price=float(r["entry_price"]),
                exit_price=float(r["exit_price"]),
                pnl=float(r["pnl"]),
                zone=r.get("zone"),
                strategy=r.get("strategy"),
                    regime=r.get("regime"),
                    source=r.get("source"),
                    backfilled=r.get("backfilled"),
                    trade_id=r.get("trade_id"),
                    position_id=r.get("position_id"),
                    decision_id=r.get("decision_id"),
                    attempt_id=r.get("attempt_id"),
                    account_id=r.get("account_id"),
                    account_name=r.get("account_name"),
                    account_mode=r.get("account_mode"),
                    account_is_practice=r.get("account_is_practice"),
                    payload_json=r.get("payload_json"),
                )
            for r in rows
        ]
    finally:
        put_conn(conn)


async def resolve_hypotheses(
    info: Info,
    status: Optional[str] = None,
    limit: int = 50,
) -> list[Hypothesis]:
    _auth(info)
    conn = _db(info)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if status:
            cur.execute(
                """SELECT id, hypothesis_id, generation, parent_hypothesis_id, claim_text, regime_context, status, created_at, updated_at
                   FROM hypotheses WHERE status = %s ORDER BY created_at DESC LIMIT %s""",
                (status, limit),
            )
        else:
            cur.execute(
                """SELECT id, hypothesis_id, generation, parent_hypothesis_id, claim_text, regime_context, status, created_at, updated_at
                   FROM hypotheses ORDER BY created_at DESC LIMIT %s""",
                (limit,),
            )
        rows = cur.fetchall()
        return [
            Hypothesis(
                id=r["id"],
                hypothesis_id=r["hypothesis_id"],
                generation=r["generation"],
                parent_hypothesis_id=r.get("parent_hypothesis_id"),
                claim_text=r["claim_text"],
                regime_context=r.get("regime_context"),
                status=r["status"],
                created_at=str(r["created_at"]) if r.get("created_at") else "",
                updated_at=str(r["updated_at"]) if r.get("updated_at") else "",
            )
            for r in rows
        ]
    finally:
        put_conn(conn)


async def resolve_similar_trades(info: Info, trade_id: int, limit: int = 10) -> list[Trade]:
    _auth(info)
    rlm_url = os.environ.get("RLM_SERVICE_URL", "http://localhost:8003").rstrip("/")
    headers = {"Authorization": f"Bearer {RLM_AUTH_TOKEN}"} if RLM_AUTH_TOKEN else {}
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{rlm_url}/similar_trades", params={"trade_id": trade_id, "limit": limit}, headers=headers)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return []
    similar = data.get("similar") or []
    return [
        Trade(
            id=t["id"],
            run_id=t["run_id"],
            inserted_at=str(t["inserted_at"]) if t.get("inserted_at") else None,
            entry_time=str(t["entry_time"]) if t.get("entry_time") else None,
            exit_time=str(t["exit_time"]) if t.get("exit_time") else None,
            direction=t["direction"],
            contracts=t["contracts"],
            entry_price=float(t["entry_price"]),
            exit_price=float(t["exit_price"]),
            pnl=float(t["pnl"]),
            zone=t.get("zone"),
            strategy=t.get("strategy"),
            regime=t.get("regime"),
            source=t.get("source"),
            backfilled=t.get("backfilled"),
            trade_id=t.get("trade_id"),
            position_id=t.get("position_id"),
            decision_id=t.get("decision_id"),
            attempt_id=t.get("attempt_id"),
            account_id=t.get("account_id"),
            account_name=t.get("account_name"),
            account_mode=t.get("account_mode"),
            account_is_practice=t.get("account_is_practice"),
            payload_json=t.get("payload_json"),
        )
        for t in similar
    ]


async def resolve_knowledge_store(
    info: Info,
    search: Optional[str] = None,
    limit: int = 50,
) -> list[KnowledgeEntry]:
    _auth(info)
    conn = _db(info)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if search:
            cur.execute(
                """SELECT id, hypothesis_id, verdict, confidence_score, mutation_directive, regime_tags, survival_count, rejection_count, created_at
                   FROM knowledge_store WHERE verdict ILIKE %s OR mutation_directive ILIKE %s ORDER BY created_at DESC LIMIT %s""",
                (f"%{search}%", f"%{search}%", limit),
            )
        else:
            cur.execute(
                """SELECT id, hypothesis_id, verdict, confidence_score, mutation_directive, regime_tags, survival_count, rejection_count, created_at
                   FROM knowledge_store ORDER BY created_at DESC LIMIT %s""",
                (limit,),
            )
        rows = cur.fetchall()
        return [
KnowledgeEntry(
                id=r["id"],
                hypothesis_id=r["hypothesis_id"],
                verdict=r["verdict"],
                confidence_score=float(r["confidence_score"]) if r.get("confidence_score") is not None else None,
                mutation_directive=r.get("mutation_directive"),
                regime_tags=r.get("regime_tags"),
                survival_count=r.get("survival_count") or 0,
                rejection_count=r.get("rejection_count") or 0,
                created_at=str(r["created_at"]) if r.get("created_at") else "",
            )
            for r in rows
        ]
    finally:
        put_conn(conn)


async def resolve_meta_learner_stats(info: Info) -> MetaLearnerStats:
    _auth(info)
    conn = _db(info)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COALESCE(SUM(survival_count), 0) AS survival_count, COALESCE(SUM(rejection_count), 0) AS rejection_count FROM knowledge_store")
        row = cur.fetchone()
        survival = int(row["survival_count"] or 0)
        rejection = int(row["rejection_count"] or 0)
        return MetaLearnerStats(
            survival_count=survival,
            rejection_count=rejection,
            stats={"survival_count": survival, "rejection_count": rejection},
        )
    finally:
        put_conn(conn)


async def resolve_reports(info: Info, limit: int = 20) -> list[AIReport]:
    _auth(info)
    conn = _db(info)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, report_id, title, report_type, model_provider, model_name, status,
                      summary_text, report_json, created_at, completed_at
               FROM ai_reports
               ORDER BY created_at DESC
               LIMIT %s""",
            (limit,),
        )
        rows = cur.fetchall()
        return [
            AIReport(
                id=r["id"],
                report_id=r["report_id"],
                title=r["title"],
                report_type=r["report_type"],
                model_provider=r["model_provider"],
                model_name=r["model_name"],
                status=r["status"],
                summary_text=r["summary_text"],
                report_json=r.get("report_json"),
                created_at=str(r["created_at"]) if r.get("created_at") else "",
                completed_at=str(r["completed_at"]) if r.get("completed_at") else None,
            )
            for r in rows
        ]
    finally:
        put_conn(conn)


async def resolve_report(info: Info, report_id: str) -> Optional[AIReport]:
    _auth(info)
    conn = _db(info)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT id, report_id, title, report_type, model_provider, model_name, status,
                      summary_text, report_json, created_at, completed_at
               FROM ai_reports
               WHERE report_id = %s""",
            (report_id,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return AIReport(
            id=r["id"],
            report_id=r["report_id"],
            title=r["title"],
            report_type=r["report_type"],
            model_provider=r["model_provider"],
            model_name=r["model_name"],
            status=r["status"],
            summary_text=r["summary_text"],
            report_json=r.get("report_json"),
            created_at=str(r["created_at"]) if r.get("created_at") else "",
            completed_at=str(r["completed_at"]) if r.get("completed_at") else None,
        )
    finally:
        put_conn(conn)
