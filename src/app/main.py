"""
Realtime demo backend.

FastAPI in front of Lakebase Postgres. Serves the latest N events for the HTML
UI to poll at ~100 ms. Per the skill's app-backend.md:
- asyncpg pool at startup (not per-request connections).
- Tiny payload — only the columns the UI renders, plus a server timestamp the
  frontend uses to show end-to-end lag.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
from fastapi import Depends, FastAPI, Query, Request
from fastapi.staticfiles import StaticFiles


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(
        host=os.environ["LAKEBASE_HOST"],
        port=int(os.environ.get("LAKEBASE_PORT", "5432")),
        database=os.environ["LAKEBASE_DB"],
        user=os.environ["LAKEBASE_USER"],
        password=os.environ["LAKEBASE_PASSWORD"],
        ssl="require",
        min_size=2,
        max_size=10,
    )
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


async def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


@app.get("/api/events/latest")
async def latest_events(
    limit: int = Query(default=50, ge=1, le=250),
    pool: asyncpg.Pool = Depends(get_pool),
):
    async with pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT id, timestamp, metric FROM events "
            "ORDER BY timestamp DESC LIMIT $1",
            limit,
        )
    rows = [dict(record) for record in records]

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
app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
