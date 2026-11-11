"""
Realtime demo backend.

FastAPI in front of Lakebase Postgres. Serves the latest N events for the HTML
UI to poll at ~100 ms. Per the skill's app-backend.md and databricks-lakebase
connectivity.md:
- OAuth auth: the app's service principal mints a short-lived Lakebase
  credential via `generate_database_credential`; a background task refreshes it
  before the ~1 h expiry, and asyncpg reads the current token whenever the pool
  opens a connection. No password and no secrets — the platform injects
  PGHOST/PGPORT/PGDATABASE/PGUSER from the bound Lakebase `postgres` resource
  (resources/realtime_demo.app.yml); LAKEBASE_ENDPOINT is set in app.yaml.
- asyncpg pool at startup (not per-request connections), pre-pinged on
  checkout so a 24x7 app never serves a dead connection.
- Tiny payload — only the columns the UI renders, plus a server timestamp the
  frontend uses to show end-to-end lag.
- A background retention loop trims `events` to the trailing RETENTION_MINUTES.
  The rate source's `id` increases monotonically, so every streamed row is an
  INSERT (append) — without this the 24x7 stream would grow the table without
  bound (~43M rows/day at 500 rps).
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
from databricks.sdk import WorkspaceClient
from fastapi import Depends, FastAPI, Query, Request
from fastapi.staticfiles import StaticFiles

# Retention: keep only the trailing window in `events`. The stream appends
# monotonic ids forever (24x7), so the table would grow unbounded without this.
RETENTION_MINUTES = float(os.environ.get("RETENTION_MINUTES", "5"))
RETENTION_INTERVAL_S = float(os.environ.get("RETENTION_INTERVAL_SECONDS", "30"))

# OAuth token refresh. Lakebase credentials live ~1 h; refresh well before that.
# After a failed refresh, retry soon rather than a full interval later, which
# would land right at the current token's expiry.
TOKEN_REFRESH_S = float(os.environ.get("LAKEBASE_TOKEN_REFRESH_SECONDS", "1800"))
TOKEN_RETRY_S = float(os.environ.get("LAKEBASE_TOKEN_RETRY_SECONDS", "60"))

# Errors meaning a pooled connection is dead: dropped past Lakebase's ~24 h
# lifetime, an endpoint bounce/failover, or a network blip.
_CONN_ERRORS = (OSError, asyncpg.PostgresError, asyncpg.InterfaceError)

_token: list[str | None] = [None]


def _mint_token(w: WorkspaceClient) -> str:
    # The app SP mints a short-lived Postgres credential for the Lakebase
    # endpoint. LAKEBASE_ENDPOINT is the full projects/.../endpoints/... path
    # set in app.yaml (the binding injects PG* params but no endpoint path).
    return w.postgres.generate_database_credential(
        endpoint=os.environ["LAKEBASE_ENDPOINT"]
    ).token


async def _token_refresh_loop(w: WorkspaceClient) -> None:
    """Re-mint the Lakebase credential before it expires. A failed refresh is
    logged and retried after TOKEN_RETRY_S, so a blip never kills the loop."""
    delay = TOKEN_REFRESH_S
    while True:
        await asyncio.sleep(delay)
        try:
            _token[0] = await asyncio.to_thread(_mint_token, w)
            delay = TOKEN_REFRESH_S
        except Exception as e:  # noqa: BLE001 — keep the loop alive
            print(f"[token] refresh failed, retrying in {TOKEN_RETRY_S:.0f}s: {e}")
            delay = TOKEN_RETRY_S


async def _ping(conn: asyncpg.Connection) -> None:
    # Pool `setup` hook, run on every checkout. A dead connection fails here and
    # asyncpg closes it, so _connection() can retry with a fresh one.
    await conn.execute("SELECT 1")


@asynccontextmanager
async def _connection(pool: asyncpg.Pool):
    """Check out a pre-pinged connection. If the ping finds a dead one, retry
    once with a fresh connection, so the 24x7 app self-heals instead of failing
    the request."""
    try:
        conn = await pool.acquire()
    except _CONN_ERRORS:
        conn = await pool.acquire()
    try:
        yield conn
    finally:
        await pool.release(conn)


async def _retention_loop(pool: asyncpg.Pool) -> None:
    """Every RETENTION_INTERVAL_S, delete rows older than RETENTION_MINUTES.

    Runs for the life of the app. Swallows transient DB errors (endpoint
    bounce, network blip) and retries on the next tick, so a hiccup never
    kills the loop. Cancelled cleanly on shutdown by the lifespan handler.
    """
    while True:
        await asyncio.sleep(RETENTION_INTERVAL_S)
        try:
            async with _connection(pool) as conn:
                status = await conn.execute(
                    "DELETE FROM events "
                    "WHERE timestamp < now() - $1::float8 * interval '1 minute'",
                    RETENTION_MINUTES,
                )
            deleted = int(status.split()[-1])  # asyncpg returns "DELETE <n>"
            if deleted:
                print(f"[retention] deleted {deleted} rows older than "
                      f"{RETENTION_MINUTES} min")
        except Exception as e:  # noqa: BLE001 — keep the loop alive
            print(f"[retention] trim failed, will retry: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # The app runs as its own service principal; WorkspaceClient() picks up the
    # injected DATABRICKS_CLIENT_ID/SECRET. Created here rather than at import,
    # so the module imports without Databricks credentials (tests, CI).
    w = WorkspaceClient()
    _token[0] = await asyncio.to_thread(_mint_token, w)
    app.state.pool = await asyncpg.create_pool(
        host=os.environ["PGHOST"],
        port=int(os.environ.get("PGPORT", "5432")),
        database=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        # Called for every new connection, so refreshed tokens are picked up
        # without rebuilding the pool.
        password=lambda: _token[0],
        ssl=os.environ.get("PGSSLMODE", "require"),
        min_size=2,
        max_size=10,
        setup=_ping,
    )
    tasks = [
        asyncio.create_task(_token_refresh_loop(w)),
        asyncio.create_task(_retention_loop(app.state.pool)),
    ]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


async def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


@app.get("/api/events/latest")
async def latest_events(
    limit: int = Query(default=50, ge=1, le=250),
    pool: asyncpg.Pool = Depends(get_pool),
):
    async with _connection(pool) as conn:
        records = await conn.fetch(
            # id DESC is only a tiebreaker: timestamp stays the primary sort so a
            # checkpoint-wipe restart (ids reset to 0) still surfaces the new
            # rows first. The tiebreaker just makes newest-N deterministic when
            # timestamps collide at 500 rps. `metric` is not selected — the UI
            # never reads it.
            "SELECT id, timestamp FROM events "
            "ORDER BY timestamp DESC, id DESC LIMIT $1",
            limit,
        )
    rows = [dict(record) for record in records]

    # Serialize timestamps to ISO with timezone so the frontend can compute lag.
    # asyncpg already returns `id` (BIGINT) as a Python int.
    for r in rows:
        ts = r["timestamp"]
        if isinstance(ts, datetime):
            r["timestamp"] = ts.astimezone(timezone.utc).isoformat()

    return {"events": rows, "server_ts": _now_iso()}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Static SPA — must mount AFTER routes so /api/* takes precedence.
app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
