"""Store contract: the API depends on this, not on a concrete implementation."""

from typing import Protocol

from ratelimit.core.bucket import Decision


class StoreUnavailableError(Exception):
    """The store cannot be reached (for example, Redis is down)."""


class RateLimitStore(Protocol):
    """Any store can deduct tokens by client key.

    The interface is asynchronous: a network store (Redis) cannot do without `await`,
    while a local one simply declares the method `async`, and the API does not notice.
    """

    capacity: float

    async def consume(self, key: str, n: float = 1) -> Decision: ...

    async def peek(self, key: str) -> float:
        """How many tokens the client has right now. Read only: deducts nothing and creates nothing."""
        ...

    async def ping(self) -> None:
        """Check that the store is reachable. Raises StoreUnavailableError if it is not."""
        ...
