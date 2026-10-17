"""State stores: one bucket per client key."""

import threading
import time
from collections.abc import Callable

from ratelimit.core.bucket import Decision, TokenBucket


class InMemoryStore:
    """In-memory store for a single process.

    Not suitable for several workers or machines: each has its own memory,
    and the limit multiplies. A shared Redis is needed there (see RedisStore).
    """

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.clock = clock
        self._buckets: dict[str, TokenBucket] = {}
        # Dict lock: protects only creating and removing buckets.
        # The tokens themselves are protected by the lock inside TokenBucket.
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._buckets)

    async def consume(self, key: str, n: float = 1) -> Decision:
        # There is no await inside: the call runs to completion without yielding control
        # to other coroutines. Threads are still possible though, so the locks stay.
        return self.consume_sync(key, n)

    def consume_sync(self, key: str, n: float = 1) -> Decision:
        # Without the lock, two threads with a new key would both miss the bucket and create their own:
        # one of them would be lost together with the token it deducted.
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self.capacity, self.refill_rate, self.clock)
                self._buckets[key] = bucket

        # Deduct outside the dict lock: clients with different keys
        # do not wait for each other, only requests for the same key conflict.
        return bucket.consume(n)

    async def peek(self, key: str) -> float:
        with self._lock:
            bucket = self._buckets.get(key)
        # Unknown client: it would have a full bucket. Do not create an entry,
        # otherwise a plain peek would bloat memory.
        return self.capacity if bucket is None else bucket.peek()

    async def ping(self) -> None:
        """Memory of the current process is always reachable."""

    def cleanup(self) -> int:
        """Remove buckets that have had time to fill up. Return how many were removed.

        A full bucket is indistinguishable from a new one, so the entry can be forgotten.
        Removing earlier is not allowed: the client would get tokens it does not have.

        Race: if a concurrent request already holds a reference to the bucket being removed,
        it will deduct a token from the "orphaned" bucket, and the client gets one extra token.
        This error is on the permissive side, and we accept it.
        """
        refill_time = self.capacity / self.refill_rate
        now = self.clock()

        with self._lock:
            # Changing a dict while iterating over it is not allowed (RuntimeError),
            # so collect the keys first, then delete.
            stale = [
                key
                for key, bucket in self._buckets.items()
                if now - bucket.last_refill >= refill_time
            ]
            for key in stale:
                del self._buckets[key]
        return len(stale)
