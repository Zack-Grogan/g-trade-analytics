# g-trade-analytics

Read-only FastAPI over Postgres for runs, events, trades, and analytics. Part of the G-Trade Railway project; see [Architecture overview](https://github.com/Zack-Grogan/G-Trade/blob/main/docs/Architecture-Overview.md) for the full architecture.

Deployed service names may still be `grogan-trade-analytics` until renamed in Railway.

- **Auth:** Single-operator (Bearer token via `ANALYTICS_API_KEY`).
- **Env:** `DATABASE_URL`, `ANALYTICS_API_KEY`

Deploy to Railway; connect to same Postgres as ingest. Web app and MCP call this API.
