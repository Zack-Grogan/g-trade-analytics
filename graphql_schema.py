"""
GraphQL schema for g-trade-analytics. Queries and advisory-only mutations (no executor changes).
"""
from __future__ import annotations

from typing import Any, Optional

import strawberry


@strawberry.type
class Run:
    run_id: str
    created_at: str
    process_id: Optional[int]
    data_mode: Optional[str]
    symbol: Optional[str]
    payload_json: Optional[strawberry.scalars.JSON]


@strawberry.type
class Trade:
    id: int
    run_id: str
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
class MetaLearnerStats:
    survival_count: int
    rejection_count: int
    stats: strawberry.scalars.JSON


@strawberry.input
class HypothesisInput:
    regime_context: Optional[str] = None
    prior_conclusions_summary: Optional[str] = None
    generation: Optional[int] = 1
    parent_hypothesis_id: Optional[str] = None


@strawberry.type
class Query:
    @strawberry.field
    async def runs(
        self,
        info: strawberry.types.Info,
        limit: int = 25,
    ) -> list[Run]:
        from .graphql_resolvers import resolve_runs
        return await resolve_runs(info, limit=limit)

    @strawberry.field
    async def run(
        self,
        info: strawberry.types.Info,
        id: str,
    ) -> Optional[Run]:
        from .graphql_resolvers import resolve_run
        return await resolve_run(info, id)

    @strawberry.field
    async def trades(
        self,
        info: strawberry.types.Info,
        run_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[Trade]:
        from .graphql_resolvers import resolve_trades
        return await resolve_trades(info, run_id=run_id, limit=limit)

    @strawberry.field
    async def hypotheses(
        self,
        info: strawberry.types.Info,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list[Hypothesis]:
        from .graphql_resolvers import resolve_hypotheses
        return await resolve_hypotheses(info, status=status, limit=limit)

    @strawberry.field
    async def similar_trades(
        self,
        info: strawberry.types.Info,
        trade_id: strawberry.ID,
        limit: int = 10,
    ) -> list[Trade]:
        from .graphql_resolvers import resolve_similar_trades
        return await resolve_similar_trades(info, int(trade_id), limit=limit)

    @strawberry.field
    async def knowledge_store(
        self,
        info: strawberry.types.Info,
        search: Optional[str] = None,
        limit: int = 50,
    ) -> list[KnowledgeEntry]:
        from .graphql_resolvers import resolve_knowledge_store
        return await resolve_knowledge_store(info, search=search, limit=limit)

    @strawberry.field
    async def meta_learner_stats(
        self,
        info: strawberry.types.Info,
    ) -> MetaLearnerStats:
        from .graphql_resolvers import resolve_meta_learner_stats
        return await resolve_meta_learner_stats(info)


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def generate_hypothesis(
        self,
        info: strawberry.types.Info,
        context: HypothesisInput,
    ) -> Optional[Hypothesis]:
        from .graphql_resolvers import resolve_generate_hypothesis
        return await resolve_generate_hypothesis(info, context)

    @strawberry.mutation
    async def submit_conclusion(
        self,
        info: strawberry.types.Info,
        result_id: strawberry.ID,
        verdict: str,
    ) -> Optional[KnowledgeEntry]:
        from .graphql_resolvers import resolve_submit_conclusion
        return await resolve_submit_conclusion(info, int(result_id), verdict)


schema = strawberry.Schema(query=Query, mutation=Mutation)
