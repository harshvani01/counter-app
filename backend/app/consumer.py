"""Kafka consumer: drains counter events and applies them to Redis.

This runs as a SEPARATE service from the API (see k8s/consumer.yaml), but from
the SAME image — it's just started with `python -m app.consumer` instead of
uvicorn. The API only *produces* events; this process *consumes* them and is the
only thing that writes the counter into Redis.

Why separate? It decouples "accepting" a request (fast, API side) from
"applying" it (here). A flood of requests piles up harmlessly in the Kafka log;
this consumer drains it at a steady pace. That's the shock-absorber / back-
pressure property Kafka gives you.
"""

import asyncio
import json
import os

import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
COUNTER_KEY = os.getenv("COUNTER_KEY", "counter")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "counter-events")
# All consumer replicas share this group id, so Kafka splits the topic's
# partitions among them and never delivers the same event to two of them.
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "counter-consumers")

# After applying an event we PUBLISH the new value to this Redis channel. The
# API's SSE endpoint subscribes to it and pushes the value out to browsers, so
# clients never have to poll. This is the "event source" for server-push.
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "counter-updates")

# Optional artificial per-event delay (milliseconds). Set >0 to deliberately
# slow the consumer and make the buffering/lag visible during a flood demo.
CONSUME_DELAY_MS = float(os.getenv("CONSUME_DELAY_MS", "0"))


async def _apply(rc: redis.Redis, op: str) -> None:
    if op == "increment":
        newval = await rc.incr(COUNTER_KEY)
    elif op == "decrement":
        newval = await rc.decr(COUNTER_KEY)
    elif op == "reset":
        await rc.set(COUNTER_KEY, 0)
        newval = 0
    else:
        print(f"[consumer] ignoring unknown op: {op!r}", flush=True)
        return
    # Notify any listening SSE streams that the value changed.
    await rc.publish(REDIS_CHANNEL, newval)


async def main() -> None:
    rc = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        enable_auto_commit=True,  # periodically save our position (offset)
        auto_offset_reset="earliest",  # a brand-new group reads from the start
        value_deserializer=lambda b: json.loads(b.decode()),
    )

    # Kafka may still be starting when we launch; retry until it's reachable.
    while True:
        try:
            await consumer.start()
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[consumer] kafka not ready ({exc}); retrying in 3s", flush=True)
            await asyncio.sleep(3)

    print(
        f"[consumer] connected; draining topic={KAFKA_TOPIC} group={KAFKA_GROUP}",
        flush=True,
    )
    try:
        async for msg in consumer:
            op = (msg.value or {}).get("op")
            await _apply(rc, op)
            if CONSUME_DELAY_MS > 0:
                await asyncio.sleep(CONSUME_DELAY_MS / 1000.0)
    finally:
        await consumer.stop()
        await rc.aclose()


if __name__ == "__main__":
    asyncio.run(main())
