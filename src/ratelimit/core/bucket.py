"""Token Bucket: lazy refill without a background timer."""

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Decision:
    """Result of a consume attempt: whether there were enough tokens and what happens next."""

    allowed: bool
    remaining: float  # how many tokens are left in the bucket
    retry_after: float  # in how many seconds the client can retry (0 if allowed)


@dataclass
class TokenBucket:
    capacity: float  # maximum tokens in the bucket (= burst size)
    refill_rate: float  # tokens per second
    clock: Callable[[], float] = time.monotonic  # time source, replaced in tests
    tokens: float = field(init=False)
    last_refill: float = field(init=False)
    # per-bucket lock: excluded from __init__, repr and comparison
    _lock: threading.Lock = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError(f"capacity must be positive, got {self.capacity}")
        if self.refill_rate <= 0:
            raise ValueError(f"refill_rate must be positive, got {self.refill_rate}")

        self.tokens = self.capacity  # the bucket starts full
        self.last_refill = self.clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Add the tokens earned since the last call. Must be called with the lock held."""
        now = self.clock()
        # max(0, ...): if the clock suddenly returns an earlier value,
        # do not take tokens away, just treat it as no time having passed.
        elapsed = max(0.0, now - self.last_refill)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def consume(self, n: float = 1) -> Decision:
        """Take n tokens if available and report what happened."""
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")

        # refill + check + deduct must be one indivisible operation,
        # otherwise two threads would take the same token twice.
        with self._lock:
            self._refill()
            if self.tokens >= n:
                self.tokens -= n
                return Decision(allowed=True, remaining=self.tokens, retry_after=0.0)

            # A rejection deducts nothing. How long until the missing tokens accumulate?
            # If n exceeds capacity, the bucket can never hold that many.
            retry_after = (
                math.inf if n > self.capacity else (n - self.tokens) / self.refill_rate
            )
            return Decision(allowed=False, remaining=self.tokens, retry_after=retry_after)

    def peek(self) -> float:
        """How many tokens are in the bucket right now. Deducts nothing."""
        with self._lock:
            self._refill()
            return self.tokens

    def try_consume(self, n: float = 1) -> bool:
        """Short form of consume: just yes or no."""
        return self.consume(n).allowed
