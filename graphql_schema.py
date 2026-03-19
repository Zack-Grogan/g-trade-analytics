"""
GraphQL schema for g-trade-analytics. Query-only read surface over Postgres.
"""
from __future__ import annotations

from typing import Optional

import strawberry


@strawberry.type
class Run:
    run_id: str
    created_at: str
    last_seen_at: Optional[str]
    process_id: Optional[int]
    data_mode: Optional[str]
    symbol: Optional[str]
    account_id: Optional[str]
    account_name: Optional[str]
    account_mode: Optional[str]
    account_is_practice: Optional[bool]
    payload_json: Optional[strawberry.scalars.JSON]


@strawberry.type
class Trade:
    id: int
    run_id: str
    inserted_at: Optional[str]
    entry_time: Optional[str]
    exit_time: Optional[str]
    direction: int
    contracts: int
    entry_price: float
    exit_price: float
    pnl: float
    zone: Optional[str]
    strategy: Optional[str]
    regime: Optional[str]
    source: Optional[str]
    backfilled: Optional[bool]
    trade_id: Optional[str]
    position_id: Optional[str]
    decision_id: Optional[str]
    attempt_id: Optional[str]
    account_id: Optional[str]
    account_name: Optional[str]
    account_mode: Optional[str]
    account_is_practice: Optional[bool]
    payload_json: Optional[strawberry.scalars.JSON]


@strawberry.type
class Hypothesis:
    id: int
    hypothesis_id: str
    generation: int
    parent_hypothesis_id: Optional[str]
    claim_text: str
    regime_context: Optional[str]
    status: str
    created_at: str
    updated_at: str


@strawberry.type
class KnowledgeEntry:
    id: int
    hypothesis_id: str
    verdict: str
    confidence_score: Optional[float]
    mutation_directive: Optional[str]
    regime_tags: Optional[strawberry.scalars.JSON]
    survival_count: int
    rejection_count: int
    created_at: str


@strawberry.type
class AIReport:
    id: int
    report_id: str
    title: str
    report_type: str
    model_provider: str
    model_name: str
    status: str
    summary_text: str
    report_json: Optional[strawberry.scalars.JSON]
    created_at: str
    completed_at: Optional[str]


@strawberry.type
class MetaLearnerStats:
    survival_count: int
    rejection_count: int
    stats: strawberry.scalars.JSON


@strawberry.type
class Query:
    @strawberry.field
    async def runs(
        self,
        info: strawberry.types.Info,
        limit: int = 25,
    ) -> list[Run]:
        from graphql_resolvers import resolve_runs
        return await resolve_runs(info, limit=limit)

    @strawberry.field
    async def run(
        self,
        info: strawberry.types.Info,
        id: str,
    ) -> Optional[Run]:
        from graphql_resolvers import resolve_run
        return await resolve_run(info, id)

    @strawberry.field
    async def trades(
        self,
        info: strawberry.types.Info,
        run_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[Trade]:
        from graphql_resolvers import resolve_trades
        return await resolve_trades(info, run_id=run_id, limit=limit)

    @strawberry.field
    async def hypotheses(
        self,
        info: strawberry.types.Info,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list[Hypothesis]:
        from graphql_resolvers import resolve_hypotheses
        return await resolve_hypotheses(info, status=status, limit=limit)

    @strawberry.field
    async def similar_trades(
        self,
        info: strawberry.types.Info,
        trade_id: strawberry.ID,
        limit: int = 10,
    ) -> list[Trade]:
        from graphql_resolvers import resolve_similar_trades
        return await resolve_similar_trades(info, int(trade_id), limit=limit)

    @strawberry.field
    async def knowledge_store(
        self,
        info: strawberry.types.Info,
        search: Optional[str] = None,
        limit: int = 50,
    ) -> list[KnowledgeEntry]:
        from graphql_resolvers import resolve_knowledge_store
        return await resolve_knowledge_store(info, search=search, limit=limit)

    @strawberry.field
    async def meta_learner_stats(
        self,
        info: strawberry.types.Info,
    ) -> MetaLearnerStats:
        from graphql_resolvers import resolve_meta_learner_stats
        return await resolve_meta_learner_stats(info)

    @strawberry.field
    async def reports(
        self,
        info: strawberry.types.Info,
        limit: int = 20,
    ) -> list[AIReport]:
        from graphql_resolvers import resolve_reports
        return await resolve_reports(info, limit=limit)

    @strawberry.field
    async def report(
        self,
        info: strawberry.types.Info,
        report_id: str,
    ) -> Optional[AIReport]:
        from graphql_resolvers import resolve_report
        return await resolve_report(info, report_id)


schema = strawberry.Schema(query=Query)
