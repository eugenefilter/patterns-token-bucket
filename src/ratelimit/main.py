"""Composition root: the only place where concrete stores are chosen.

Run:
    uvicorn ratelimit.main:app --reload
    RATELIMIT_BACKEND=redis uvicorn ratelimit.main:app --workers 4

Simulator page: http://localhost:8000/
"""

import uuid

from redis.asyncio import BlockingConnectionPool, Redis

from ratelimit.api.app import create_app
from ratelimit.config import Settings
from ratelimit.sim.simulator import SimConfig, SimulationManager, StoreFactory
from ratelimit.storage.base import RateLimitStore
from ratelimit.storage.memory import InMemoryStore
from ratelimit.storage.redis_store import NaiveRedisStore, RedisStore


def build_redis(settings: Settings) -> Redis:
    # Blocking pool: during a spike requests wait for a connection instead of failing.
    pool = BlockingConnectionPool.from_url(settings.redis_url, max_connections=50)
    return Redis(connection_pool=pool)


def build_store(settings: Settings) -> RateLimitStore:
    if settings.backend == "redis":
        return RedisStore(build_redis(settings), settings.capacity, settings.refill_rate)
    return InMemoryStore(settings.capacity, settings.refill_rate)


def build_sim_store_factory(settings: Settings) -> StoreFactory:
    """Store for the simulator: the page picks parameters and backend, not env."""
    client: Redis | None = None  # created lazily: not every run needs Redis

    def factory(config: SimConfig) -> RateLimitStore:
        nonlocal client
        if config.backend == "memory":
            return InMemoryStore(config.capacity, config.refill_rate)

        client = client or build_redis(settings)
        # Unique prefix per run: runs do not see each other's state.
        prefix = f"sim:{uuid.uuid4().hex}:"
        store_cls = RedisStore if config.backend == "redis" else NaiveRedisStore
        return store_cls(client, config.capacity, config.refill_rate, prefix=prefix)

    return factory


_settings = Settings()
app = create_app(
    build_store(_settings),
    SimulationManager(build_sim_store_factory(_settings)),
)
