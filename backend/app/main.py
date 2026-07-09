import os

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
COUNTER_KEY = os.getenv("COUNTER_KEY", "counter")

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


class CounterResponse(BaseModel):
    value: int


@app.on_event("startup")
async def startup() -> None:
    global redis_client
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    if redis_client is not None:
        await redis_client.aclose()


def _client() -> redis.Redis:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis client not initialised")
    return redis_client


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
