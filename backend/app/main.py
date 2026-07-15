import asyncio
import os

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
COUNTER_KEY = os.getenv("COUNTER_KEY", "counter")

# PostgreSQL: the durable "system of record". The backend periodically snapshots
# the fast Redis counter into Postgres so the value survives Redis being wiped.
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "counter")
PG_USER = os.getenv("PG_USER", "counter")
PG_PASSWORD = os.getenv("PG_PASSWORD", "counter")
# How often (seconds) to snapshot Redis -> Postgres.
SNAPSHOT_INTERVAL = float(os.getenv("SNAPSHOT_INTERVAL", "5"))

app = FastAPI(title="Counter API", version="1.0.0")

# CORS is permissive here because the frontend may be served from a different
# origin during local development. In-cluster, Traefik serves both under one host.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client: redis.Redis | None = None
pg_pool: asyncpg.Pool | None = None
_snapshot_task: asyncio.Task | None = None


class CounterResponse(BaseModel):
    value: int


@app.on_event("startup")
async def startup() -> None:
    global redis_client, _snapshot_task
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )
    # Best-effort: connect to Postgres, ensure the table exists, and if Redis
    # has no counter yet, restore it from the last durable snapshot.
    await _ensure_pg()
    await _restore_from_pg()
    # Start the background loop that snapshots Redis -> Postgres on an interval.
    _snapshot_task = asyncio.create_task(_snapshot_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    if _snapshot_task is not None:
        _snapshot_task.cancel()
    if pg_pool is not None:
        await pg_pool.close()
    if redis_client is not None:
        await redis_client.aclose()


def _client() -> redis.Redis:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis client not initialised")
    return redis_client


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS counter_snapshots (
    id         BIGSERIAL PRIMARY KEY,
    value      BIGINT      NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


async def _ensure_pg() -> asyncpg.Pool | None:
    """Lazily create the Postgres pool + table. Returns None if Postgres is
    down, so the app keeps serving from Redis (graceful degradation)."""
    global pg_pool
    if pg_pool is not None:
        return pg_pool
    try:
        pg_pool = await asyncpg.create_pool(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DB,
            min_size=1,
            max_size=5,
        )
        async with pg_pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
        return pg_pool
    except Exception as exc:  # noqa: BLE001
        print(f"[persistence] Postgres unavailable, will retry: {exc}", flush=True)
        pg_pool = None
        return None


async def _restore_from_pg() -> None:
    """If Redis has no counter yet (e.g. it was wiped), seed it from the last
    durable snapshot in Postgres."""
    pool = await _ensure_pg()
    if pool is None:
        return
    try:
        if await _client().get(COUNTER_KEY) is not None:
            return  # Redis already has a value; nothing to restore.
        row = await pool.fetchrow(
            "SELECT value FROM counter_snapshots ORDER BY id DESC LIMIT 1"
        )
        if row is not None:
            await _client().set(COUNTER_KEY, row["value"])
            print(
                f"[persistence] restored counter={row['value']} from Postgres",
                flush=True,
            )
    except Exception as exc:  # noqa: BLE001
        print(f"[persistence] restore failed: {exc}", flush=True)


async def _snapshot_loop() -> None:
    """Every SNAPSHOT_INTERVAL seconds, write the current Redis counter into
    Postgres as a new durable history row."""
    while True:
        await asyncio.sleep(SNAPSHOT_INTERVAL)
        try:
            pool = await _ensure_pg()
            if pool is None:
                continue
            value = await _client().get(COUNTER_KEY)
            value = int(value) if value is not None else 0
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO counter_snapshots (value) VALUES ($1)", value
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[persistence] snapshot failed: {exc}", flush=True)


@app.get("/api/counter", response_model=CounterResponse)
async def get_counter() -> CounterResponse:
    value = await _client().get(COUNTER_KEY)
    return CounterResponse(value=int(value) if value is not None else 0)


@app.post("/api/counter/increment", response_model=CounterResponse)
async def increment_counter() -> CounterResponse:
    value = await _client().incr(COUNTER_KEY)
    return CounterResponse(value=value)


@app.post("/api/counter/decrement", response_model=CounterResponse)
async def decrement_counter() -> CounterResponse:
    value = await _client().decr(COUNTER_KEY)
    return CounterResponse(value=value)


@app.post("/api/counter/reset", response_model=CounterResponse)
async def reset_counter() -> CounterResponse:
    await _client().set(COUNTER_KEY, 0)
    return CounterResponse(value=0)


@app.get("/api/history")
async def history(limit: int = 20) -> dict:
    """Recent durable snapshots from Postgres (newest first)."""
    pool = await _ensure_pg()
    if pool is None:
        return {"persistence": "unavailable", "snapshots": []}
    rows = await pool.fetch(
        "SELECT value, created_at FROM counter_snapshots ORDER BY id DESC LIMIT $1",
        limit,
    )
    return {
        "persistence": "ok",
        "snapshots": [
            {"value": r["value"], "at": r["created_at"].isoformat()} for r in rows
        ],
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    try:
        await _client().ping()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}")
    return {"status": "ready"}
