from concurrent.futures import ThreadPoolExecutor

import pytest

from ratelimit.storage.memory import InMemoryStore


def test_keys_have_independent_buckets(clock):
    store = InMemoryStore(capacity=1, refill_rate=1, clock=clock)
    assert store.consume_sync("alice").allowed is True
    assert store.consume_sync("alice").allowed is False
    assert store.consume_sync("bob").allowed is True


def test_cleanup_removes_only_full_buckets(clock):
    store = InMemoryStore(capacity=10, refill_rate=1, clock=clock)  # full in 10 s
    store.consume_sync("idle")
    clock.advance(6)
    store.consume_sync("active")
    clock.advance(6)  # idle has been silent for 12 s, active for 6 s
    assert store.cleanup() == 1
    assert len(store) == 1


def test_cleanup_does_not_give_free_tokens(clock):
    store = InMemoryStore(capacity=2, refill_rate=1, clock=clock)
    store.consume_sync("a", 2)
    clock.advance(1)  # the bucket is not full yet
    assert store.cleanup() == 0
    assert store.consume_sync("a", 2).allowed is False  # 1 token is left, not 2


def test_concurrent_requests_never_exceed_capacity(clock):
    store = InMemoryStore(capacity=100, refill_rate=1, clock=clock)
    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(lambda _: store.consume_sync("shared").allowed, range(500)))
    assert sum(results) == 100


@pytest.mark.anyio
async def test_async_consume_reports_remaining_and_retry_after(clock):
    store = InMemoryStore(capacity=2, refill_rate=1, clock=clock)
    assert (await store.consume("a")).remaining == 1
    await store.consume("a")
    denied = await store.consume("a")
    assert denied.allowed is False
    assert denied.retry_after == 1


@pytest.mark.anyio
async def test_peek_does_not_create_buckets(clock):
    store = InMemoryStore(capacity=5, refill_rate=1, clock=clock)
    assert await store.peek("stranger") == 5  # unknown client: the bucket is full
    assert len(store) == 0


@pytest.mark.anyio
async def test_peek_matches_state_after_consume(clock):
    store = InMemoryStore(capacity=5, refill_rate=1, clock=clock)
    store.consume_sync("a", 3)
    assert await store.peek("a") == 2
