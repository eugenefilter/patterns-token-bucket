"""Race demonstration: 500 simultaneous requests against a 100-token bucket.

Run: .venv/bin/python scripts/race_demo.py
Requires a running Redis (brew services start redis).
The database comes from RATELIMIT_TEST_REDIS_URL (default: database 15).
"""

import asyncio
import uuid

from redis.asyncio import BlockingConnectionPool, Redis

from ratelimit.config import Settings
from ratelimit.storage.redis_store import NaiveRedisStore, RedisStore

CAPACITY = 100
REQUESTS = 500


async def run(store_cls: type, client: Redis) -> int:
    # Very slow refill: the tokens barely grow during the test.
    store = store_cls(client, capacity=CAPACITY, refill_rate=0.001, prefix=f"demo:{uuid.uuid4()}:")
    decisions = await asyncio.gather(*(store.consume("client") for _ in range(REQUESTS)))
    return sum(d.allowed for d in decisions)


async def main() -> None:
    # BlockingConnectionPool: requests beyond the limit wait for a connection
    # instead of failing with MaxConnectionsError, as in the default pool.
    pool = BlockingConnectionPool.from_url(Settings().test_redis_url, max_connections=50)
    client = Redis(connection_pool=pool)
    try:
        print(f"Bucket of {CAPACITY} tokens, {REQUESTS} simultaneous requests.\n")
        for store_cls in (NaiveRedisStore, RedisStore):
            allowed = await run(store_cls, client)
            verdict = "correct" if allowed == CAPACITY else f"RACE: {allowed - CAPACITY} too many"
            print(f"{store_cls.__name__:<16} passed {allowed:>3}  ({verdict})")
    finally:
        # Remove only our own keys: the URL is configurable, so never flush the whole database.
        async for key in client.scan_iter("demo:*"):
            await client.delete(key)
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
