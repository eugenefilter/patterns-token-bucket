"""Load simulator: virtual clients send requests to a real store.

This is not an animation: every request goes through the same RateLimitStore
that protects the API.

A token bucket itself knows no queue: it lets a request through or answers 429.
A queue appears on the client side if the client waits for Retry-After on a 429
and retries the request ("retry" mode). In "drop" mode the client does not retry a 429.
"""

import asyncio
import math
import random
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from ratelimit.storage.base import RateLimitStore

MAX_CLIENTS = 1000
MAX_TOTAL_RPS = 3000  # above this one asyncio process can no longer keep the chart smooth
SHOWN_CLIENTS = 50  # how many clients of a group are shown individually
MAX_QUEUE = 10_000  # one client's queue length: beyond it requests are dropped
EVENTS_KEPT = 500  # how many latest events the server remembers
EVENTS_SENT = 40  # how many latest events go to the page in one frame


class GroupConfig(BaseModel):
    """A group of identical clients with the same request rate.

    By default every client has its own bucket. With shared_key all clients of the group
    use one key and share one bucket (like several servers with one API key).
    """

    name: str = Field(min_length=1, max_length=30)
    count: int = Field(1, ge=1, le=MAX_CLIENTS)
    rps: int = Field(ge=1, le=200, description="requests per second from a single client")
    shared_key: bool = False
    on_limit: Literal["drop", "retry"] = Field(
        "drop", description="what a client does on a 429: drop the request or wait and retry"
    )


class SimConfig(BaseModel):
    capacity: float = Field(gt=0, le=1000)
    refill_rate: float = Field(gt=0, le=1000)
    backend: Literal["memory", "redis", "redis-naive"] = "memory"
    groups: list[GroupConfig] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def _check_total_load(self) -> "SimConfig":
        clients = sum(g.count for g in self.groups)
        if clients > MAX_CLIENTS:
            raise ValueError(f"at most {MAX_CLIENTS} clients in total, got {clients}")
        total_rps = sum(g.count * g.rps for g in self.groups)
        if total_rps > MAX_TOTAL_RPS:
            raise ValueError(f"total load must not exceed {MAX_TOTAL_RPS} req/s, got {total_rps:g}")
        return self


# How to build a store for the chosen settings: decided by the composition root (main.py),
# the simulator knows nothing about concrete implementations.
StoreFactory = Callable[[SimConfig], RateLimitStore]


@dataclass
class _ClientState:
    sent: int = 0  # new requests made (retries after a 429 are not counted)
    passed: int = 0  # requests that passed
    rejected: int = 0  # 429 responses received (in retry mode one request can get several)
    queue: deque[float] = field(default_factory=deque)  # arrival times of waiting requests


@dataclass
class _GroupState:
    config: GroupConfig
    clients: list[_ClientState]

    def total(self, attr: str) -> int:
        return sum(getattr(c, attr) for c in self.clients)

    @property
    def waiting(self) -> int:
        return sum(len(c.queue) for c in self.clients)


class Simulation:
    def __init__(self, config: SimConfig, store: RateLimitStore) -> None:
        self.config = config
        self._store = store
        self._groups: list[_GroupState] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._events: deque[dict[str, Any]] = deque(maxlen=EVENTS_KEPT)
        self._seq = 0
        self._started = 0.0
        self._stopped_at: float | None = None
        self.errors = 0

    @property
    def running(self) -> bool:
        return self._stopped_at is None

    def _key(self, gi: int, ci: int) -> str:
        """Client key in the store; for a group with shared_key it is the same for everyone."""
        return f"g{gi}" if self._groups[gi].config.shared_key else f"g{gi}:{ci}"

    def start(self) -> None:
        self._started = time.monotonic()
        for group_config in self.config.groups:
            clients = [_ClientState() for _ in range(group_config.count)]
            self._groups.append(_GroupState(group_config, clients))
        for gi, group in enumerate(self._groups):
            for ci in range(group.config.count):
                self._tasks.append(asyncio.create_task(self._client_loop(gi, ci)))

    async def stop(self) -> None:
        if not self.running:
            return
        self._stopped_at = time.monotonic()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def _log(self, gi: int, ci: int, status: int, now: float, **extra: float) -> None:
        self._seq += 1
        self._events.append(
            {"seq": self._seq, "t": now - self._started, "g": gi, "c": ci, "status": status, **extra}
        )

    async def _client_loop(self, gi: int, ci: int) -> None:
        """One virtual client: new requests appear on a schedule,
        and are sent from the queue one at a time."""
        group = self._groups[gi]
        client = group.clients[ci]
        key = self._key(gi, ci)
        retry = group.config.on_limit == "retry"
        interval = 1 / group.config.rps

        # Random start phase: otherwise all clients of a group would fire at the same moment.
        next_arrival = time.monotonic() + random.uniform(0, interval)
        next_attempt = math.inf  # when the request at the head of the queue may be sent

        while True:
            now = time.monotonic()
            wake = min(next_arrival, next_attempt)
            if wake > now:
                await asyncio.sleep(wake - now)
            else:
                if now - wake > 1:  # fell far behind: do not catch up on what was missed
                    next_arrival = now
                await asyncio.sleep(0)  # give other coroutines a chance
            now = time.monotonic()

            if now >= next_arrival:  # the client decided to make a new request
                client.sent += 1
                if len(client.queue) >= MAX_QUEUE:
                    client.rejected += 1  # queue is full: the request is dropped
                else:
                    client.queue.append(now)
                    if len(client.queue) == 1:
                        next_attempt = now
                next_arrival += interval

            if client.queue and now >= next_attempt:  # try to send the request at the head
                try:
                    decision = await self._store.consume(key)
                except Exception:
                    self.errors += 1  # e.g. Redis is unavailable: count it and carry on
                    if retry:
                        next_attempt = now + 0.5
                    else:
                        client.queue.popleft()
                        next_attempt = now if client.queue else math.inf
                    continue

                now = time.monotonic()
                if decision.allowed:
                    waited = now - client.queue.popleft()
                    client.passed += 1
                    self._log(gi, ci, 200, now, waited=waited)
                    next_attempt = now if client.queue else math.inf
                else:
                    client.rejected += 1
                    self._log(gi, ci, 429, now, retry_after=decision.retry_after)
                    if retry:  # the request stays in the queue and waits for Retry-After
                        next_attempt = now + max(decision.retry_after, 0.01)
                    else:  # the request is lost
                        client.queue.popleft()
                        next_attempt = now if client.queue else math.inf

    def snapshot(self) -> dict[str, Any]:
        """Current state for the page."""
        now = self._stopped_at if self._stopped_at is not None else time.monotonic()
        groups = []
        for g in self._groups:
            shown = min(g.config.count, SHOWN_CLIENTS)
            ages = [now - c.queue[0] for c in g.clients if c.queue]
            groups.append(
                {
                    "name": g.config.name,
                    "count": g.config.count,
                    "rps": g.config.rps,
                    "shared": g.config.shared_key,
                    "on_limit": g.config.on_limit,
                    "sent": g.total("sent"),
                    "passed": g.total("passed"),
                    "rejected": g.total("rejected"),
                    "waiting": g.waiting,
                    "oldest": max(ages, default=0.0),  # how long the oldest request in the group has been waiting
                    "shown": shown,
                    "clients": [
                        {
                            "sent": c.sent,
                            "passed": c.passed,
                            "rejected": c.rejected,
                            "waiting": len(c.queue),
                            "oldest": now - c.queue[0] if c.queue else 0.0,
                        }
                        for c in g.clients[:shown]
                    ],
                }
            )
        return {
            "running": self.running,
            "t": now - self._started,
            "errors": self.errors,
            "config": {
                "capacity": self.config.capacity,
                "refill_rate": self.config.refill_rate,
                "backend": self.config.backend,
            },
            "groups": groups,
            "events": list(self._events)[-EVENTS_SENT:],
        }


class SimulationManager:
    """Holds the current simulation: at most one runs at a time."""

    def __init__(self, store_factory: StoreFactory) -> None:
        self._factory = store_factory
        self._current: Simulation | None = None
        self._lock = asyncio.Lock()

    @property
    def current(self) -> Simulation | None:
        return self._current

    async def start(self, config: SimConfig) -> None:
        async with self._lock:
            store = self._factory(config)
            await store.ping()  # fail early and clearly, instead of counting errors later
            if self._current:
                await self._current.stop()
            sim = Simulation(config, store)
            sim.start()
            self._current = sim

    async def stop(self) -> None:
        async with self._lock:
            if self._current:
                await self._current.stop()  # the last snapshot stays available to the page
