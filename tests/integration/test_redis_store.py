"""RedisStore tests. A running Redis is required, otherwise the tests are skipped."""

import asyncio
import math
import uuid

import pytest
from redis.asyncio import BlockingConnectionPool, Redis

from ratelimit.config import Settings
from ratelimit.storage.base import StoreUnavailableError
from ratelimit.storage.redis_store import RedisStore

pytestmark = pytest.mark.anyio

# RATELIMIT_TEST_REDIS_URL (or .env): a separate database so real data is untouched
TEST_DB_URL = Settings().test_redis_url


@pytest.fixture
async def redis_client():
    pool = BlockingConnectionPool.from_url(TEST_DB_URL, max_connections=50)
    client = Redis(connection_pool=pool)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("Redis is unavailable")
    yield client
    async for key in client.scan_iter("test:*"):
        await client.delete(key)
    await client.aclose()


@pytest.fixture
def make_store(redis_client):
    # Unique prefix per test: tests do not see each other's data.
    def factory(capacity: float, refill_rate: float) -> RedisStore:
        return RedisStore(redis_client, capacity, refill_rate, prefix=f"test:{uuid.uuid4()}:")

    return factory


async def test_concurrent_requests_never_exceed_capacity(make_store):
    store = make_store(capacity=100, refill_rate=0.001)
    decisions = await asyncio.gather(*(store.consume("shared") for _ in range(500)))
    assert sum(d.allowed for d in decisions) == 100


async def test_keys_are_independent(make_store):
    store = make_store(capacity=1, refill_rate=0.001)
    assert (await store.consume("alice")).allowed is True
    assert (await store.consume("alice")).allowed is False
    assert (await store.consume("bob")).allowed is True


async def test_refills_over_time(make_store):
    store = make_store(capacity=2, refill_rate=20)  # a token returns in 50 ms
    await store.consume("a", 2)
    assert (await store.consume("a")).allowed is False
    await asyncio.sleep(0.1)
    assert (await store.consume("a")).allowed is True


async def test_remaining_keeps_fraction(make_store):
    store = make_store(capacity=3, refill_rate=0.001)
    decision = await store.consume("a")
    assert decision.remaining == pytest.approx(2, abs=0.01)  # Lua does not truncate to an integer


async def test_denied_reports_finite_retry_after(make_store):
    store = make_store(capacity=1, refill_rate=0.5)  # a token returns in 2 s
    await store.consume("a")
    denied = await store.consume("a")
    assert denied.allowed is False
    assert 0 < denied.retry_after <= 2


async def test_request_larger_than_capacity_never_succeeds(make_store):
    store = make_store(capacity=2, refill_rate=1)
    denied = await store.consume("a", 5)
    assert denied.allowed is False
    assert math.isinf(denied.retry_after)


async def test_key_expires_when_bucket_would_be_full(make_store, redis_client):
    store = make_store(capacity=10, refill_rate=1)  # full in 10 s
    await store.consume("a")
    ttl_ms = await redis_client.pttl(store._prefix + "a")
    assert 0 < ttl_ms <= 10_000


async def test_peek_reads_without_spending_or_creating(make_store, redis_client):
    store = make_store(capacity=5, refill_rate=0.001)
    assert await store.peek("stranger") == 5
    assert await redis_client.exists(store._prefix + "stranger") == 0  # the key was not created

    await store.consume("a", 2)
    assert await store.peek("a") == pytest.approx(3, abs=0.01)
    assert await store.peek("a") == pytest.approx(3, abs=0.01)  # the repeat deducted nothing


async def test_ping_succeeds_when_redis_is_up(make_store):
    await make_store(capacity=1, refill_rate=1).ping()


async def test_ping_reports_unreachable_redis():
    client = Redis.from_url("redis://localhost:1/0", socket_connect_timeout=0.5)  # nothing listens on port 1
    store = RedisStore(client, capacity=1, refill_rate=1)
    with pytest.raises(StoreUnavailableError, match="Redis is unavailable"):
        await store.ping()
    await client.aclose()
