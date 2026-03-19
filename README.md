# g-trade-analytics

Read-only FastAPI over Postgres for runs, events, bridge diagnostics, runtime logs, and analytics artifacts. Part of the G-Trade Railway project; see [Architecture overview](https://github.com/Zack-Grogan/G-Trade/blob/main/docs/Architecture-Overview.md) for the full architecture.

Deployed service names may still be `grogan-trade-analytics` until renamed in Railway.

- **Auth:** Single-operator Bearer token via `ANALYTICS_API_KEY` or `GTRADE_INTERNAL_API_TOKEN`. This applies to REST routes and query-only `/graphql`.
- **Env:** `DATABASE_URL`, `ANALYTICS_API_KEY`
- **Optional env:** `GTRADE_INTERNAL_API_TOKEN` for shared internal machine auth, `RLM_SERVICE_URL` for RLM-linked query helpers, `RLM_AUTH_TOKEN` if the RLM service itself is bearer-protected.
- **Read surfaces:** run history, timelines, bridge failures, service health, run-scoped runtime logs, global runtime-log search, account summaries, account-trade ledger views, reports, and hypotheses.

Deploy to Railway; connect to the same Postgres as ingest. Web and MCP read from this API. Analytics is query-only; RLM owns report/hypothesis generation writes.

The analytics read model is now account-aware. Runs, state snapshots, manifests, and trade views expose `account_id`, `account_name`, and practice/live mode, and the REST API exposes dedicated `/accounts` and `/account-trades` surfaces for operator use.
