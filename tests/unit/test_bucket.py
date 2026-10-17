import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

from ratelimit.core.bucket import TokenBucket


def test_starts_full(clock):
    bucket = TokenBucket(capacity=5, refill_rate=1, clock=clock)
    assert bucket.tokens == 5


def test_allows_burst_up_to_capacity(clock):
    bucket = TokenBucket(capacity=3, refill_rate=1, clock=clock)
    assert [bucket.try_consume() for _ in range(4)] == [True, True, True, False]


def test_rejected_request_does_not_spend_tokens(clock):
    bucket = TokenBucket(capacity=2, refill_rate=1, clock=clock)
    assert bucket.try_consume(5) is False
    assert bucket.tokens == 2


def test_refills_over_time(clock):
    bucket = TokenBucket(capacity=2, refill_rate=1, clock=clock)
    bucket.try_consume(2)
    assert bucket.try_consume() is False
    clock.advance(1)
    assert bucket.try_consume() is True


def test_refill_is_fractional(clock):
    bucket = TokenBucket(capacity=10, refill_rate=2, clock=clock)
    bucket.try_consume(10)
    clock.advance(0.25)
    assert bucket.try_consume() is False  # 0.5 tokens have accumulated
    clock.advance(0.25)
    assert bucket.try_consume() is True  # 1 token has accumulated


def test_never_exceeds_capacity(clock):
    bucket = TokenBucket(capacity=3, refill_rate=1, clock=clock)
    clock.advance(1000)
    bucket.try_consume(0)  # just trigger a refill
    assert bucket.tokens == 3


@pytest.mark.parametrize(
    ("capacity", "refill_rate"),
    [(0, 1), (-1, 1), (1, 0), (1, -5)],
)
def test_rejects_invalid_arguments(capacity, refill_rate):
    with pytest.raises(ValueError):
        TokenBucket(capacity=capacity, refill_rate=refill_rate)


@pytest.fixture
def frequent_thread_switches():
    """Threads switch more often: the race shows up more reliably."""
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    yield
    sys.setswitchinterval(old)


def test_concurrent_consume_never_exceeds_capacity(clock, frequent_thread_switches):
    bucket = TokenBucket(capacity=100, refill_rate=1, clock=clock)
    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(lambda _: bucket.try_consume(), range(2000)))
    assert sum(results) == 100


def test_peek_reports_tokens_without_spending(clock):
    bucket = TokenBucket(capacity=4, refill_rate=1, clock=clock)
    bucket.try_consume(3)
    assert bucket.peek() == 1
    assert bucket.peek() == 1  # peeking again changes nothing
    clock.advance(2)
    assert bucket.peek() == 3  # but time is taken into account
