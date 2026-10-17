"""Redis stores: shared state for all workers and machines.

There are two classes with the same algorithm but different atomicity:

* NaiveRedisStore: reads, computes in Python, writes back. Contains a race.
    Only meant as a demonstration, it must not be wired into the API.
* RedisStore: the whole algorithm runs inside Redis as a single Lua script.
"""

import math
import time
from collections.abc import Callable

from redis.asyncio import Redis

from ratelimit.core.bucket import Decision
from ratelimit.storage.base import StoreUnavailableError

# The whole Token Bucket algorithm inside Redis. The script runs atomically:
# while it runs, Redis serves no other clients.
#
# Time comes from Redis itself (TIME), not from the application: clocks on
# different machines drift apart, while the time server is shared by all workers.
#
# Lua numbers are truncated to integers when returned to the client, so
# fractional values are returned as strings.
_CONSUME_LUA = """
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local n        = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local data   = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
    tokens = capacity  -- a new client starts with a full bucket
    ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
local retry_after = 0
if tokens >= n then
    tokens = tokens - n
    allowed = 1
elseif n > capacity then
    retry_after = -1  -- the bucket can never hold that many
else
    retry_after = (n - tokens) / rate
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
-- After this time the bucket would be full anyway, so the entry can be forgotten.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000))

return {allowed, tostring(tokens), tostring(retry_after)}
"""

# Read only: the same calculations, but the state is not changed.
_PEEK_LUA = """
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local data   = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
    return tostring(capacity)  -- unknown client: the bucket would be full
end

local elapsed = math.max(0, now - ts)
return tostring(math.min(capacity, tokens + elapsed * rate))
"""


async def _ping(client: Redis) -> None:
    try:
        await client.ping()
    except Exception as exc:  # connection refused, timeout, bad credentials, ...
        raise StoreUnavailableError(f"Redis is unavailable: {exc}") from exc


def _decision(allowed: bool, tokens: float, retry_after: float) -> Decision:
    return Decision(allowed=allowed, remaining=tokens, retry_after=retry_after)


class RedisStore:
    """Atomic store: one Lua script per request, one network round trip."""

    def __init__(
        self,
        client: Redis,
        capacity: float,
        refill_rate: float,
        prefix: str = "rl:",
    ) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._prefix = prefix
        # register_script uploads the script once and then calls it by hash (EVALSHA).
        self._client = client
        self._consume = client.register_script(_CONSUME_LUA)
        self._peek = client.register_script(_PEEK_LUA)

    async def consume(self, key: str, n: float = 1) -> Decision:
        allowed, tokens, retry_after = await self._consume(
            keys=[self._prefix + key],
            args=[self.capacity, self.refill_rate, n],
        )
        retry = float(retry_after)
        return _decision(
            allowed=bool(allowed),
            tokens=float(tokens),
            retry_after=math.inf if retry < 0 else retry,
        )

    async def ping(self) -> None:
        await _ping(self._client)

    async def peek(self, key: str) -> float:
        tokens = await self._peek(keys=[self._prefix + key], args=[self.capacity, self.refill_rate])
        return float(tokens)


class NaiveRedisStore:
    """The reference for how NOT to do it: there is a race window between read and write.

    Two requests read the same number of tokens at the same time,
    both decide that deducting is possible, and both write the result. One token is spent,
    but two requests are let through.
    """

    def __init__(
        self,
        client: Redis,
        capacity: float,
        refill_rate: float,
        prefix: str = "rl:",
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._client = client
        self._prefix = prefix
        self._clock = clock

    async def consume(self, key: str, n: float = 1) -> Decision:
        redis_key = self._prefix + key
        now = self._clock()

        # 1. read
        tokens_raw, ts_raw = await self._client.hmget(redis_key, "tokens", "ts")
        tokens = float(tokens_raw) if tokens_raw is not None else self.capacity
        ts = float(ts_raw) if ts_raw is not None else now

        # 2. compute in Python (other requests manage to read the same value here)
        tokens = min(self.capacity, tokens + max(0.0, now - ts) * self.refill_rate)
        allowed = tokens >= n
        if allowed:
            tokens -= n
        retry_after = 0.0 if allowed else (n - tokens) / self.refill_rate

        # 3. write
        await self._client.hset(redis_key, mapping={"tokens": tokens, "ts": now})
        await self._client.pexpire(redis_key, math.ceil(self.capacity / self.refill_rate * 1000))
        return _decision(allowed, tokens, retry_after)

    async def peek(self, key: str) -> float:
        tokens_raw, ts_raw = await self._client.hmget(self._prefix + key, "tokens", "ts")
        if tokens_raw is None:
            return self.capacity
        elapsed = max(0.0, self._clock() - float(ts_raw))
        return min(self.capacity, float(tokens_raw) + elapsed * self.refill_rate)

    async def ping(self) -> None:
        await _ping(self._client)
