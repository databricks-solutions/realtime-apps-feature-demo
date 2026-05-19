"""
Realtime demo backend.

FastAPI in front of Lakebase Postgres. Serves the latest N events for the React
UI to poll at ~100 ms. Per the skill's app-backend.md:
- AsyncConnectionPool at startup (not per-request connections).
- Tiny payload — only the columns the UI renders, plus a server timestamp the
  frontend uses to show end-to-end lag.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Request
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


def conninfo() -> str:
    return (
        f"host={os.environ['LAKEBASE_HOST']} "
        f"port={os.environ.get('LAKEBASE_PORT', '5432')} "
        f"dbname={os.environ['LAKEBASE_DB']} "
        f"user={os.environ['LAKEBASE_USER']} "
        f"password={os.environ['LAKEBASE_PASSWORD']} "
        f"sslmode=require"
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = AsyncConnectionPool(
        conninfo=conninfo(),
        min_size=2,
        max_size=10,
        kwargs={"row_factory": dict_row},
        open=False,
    )
    await app.state.pool.open()
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


async def get_pool(request: Request) -> AsyncConnectionPool:
    return request.app.state.pool


@app.get("/api/events/latest")
async def latest_events(
    limit: int = 50,
    pool: AsyncConnectionPool = Depends(get_pool),
):
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT id, timestamp, metric FROM events "
            "ORDER BY timestamp DESC LIMIT %s",
            (limit,),
        )
        rows = await cur.fetchall()

    # Serialize timestamps to ISO with timezone so the frontend can compute lag.
    for r in rows:
        ts = r["timestamp"]
        if isinstance(ts, datetime):
            r["timestamp"] = ts.astimezone(timezone.utc).isoformat()
        r["metric"] = float(r["metric"]) if r["metric"] is not None else None
        r["id"] = int(r["id"])

    return {"events": rows, "server_ts": _now_iso()}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Static SPA — must mount AFTER routes so /api/* takes precedence.
app.mount("/", StaticFiles(directory="static", html=True), name="static")
