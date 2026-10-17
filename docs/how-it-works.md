# How the mechanism works

The Token Bucket pattern itself lives in a single file: `src/ratelimit/core/bucket.py`, class `TokenBucket`. It is about 76 lines long and contains no HTTP and no Redis. Everything else in the project exists so that this class can work under real conditions.

## Bucket state

| Field | Meaning |
|---|---|
| `capacity` | Bucket size: the maximum number of tokens it can hold |
| `refill_rate` | How many tokens return per second |
| `tokens` | How many tokens are in the bucket right now |
| `last_refill` | When `tokens` was last recomputed |
| `clock` | The time source. `time.monotonic` in production, replaced in tests |

Two mutable numbers are stored per client: `tokens` and `last_refill`. A bucket is created full: `tokens = capacity`.

## Step 1. Refill (`_refill`)

No background timer is needed. Tokens are recomputed at the moment of a request, from the time that has passed:

```python
def _refill(self) -> None:
    now = self.clock()
    elapsed = max(0.0, now - self.last_refill)
    self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
    self.last_refill = now
```

- `elapsed * refill_rate` is what has accumulated during the pause.
- `min(self.capacity, ...)` keeps the bucket from overflowing, however long the client stays silent.
- `max(0.0, ...)` guards against the clock returning an earlier value: time cannot go backwards.

## Step 2. Decision (`consume`)

```python
def consume(self, n: float = 1) -> Decision:
    with self._lock:
        self._refill()
        if self.tokens >= n:
            self.tokens -= n
            return Decision(allowed=True, remaining=self.tokens, retry_after=0.0)

        retry_after = (
            math.inf if n > self.capacity else (n - self.tokens) / self.refill_rate
        )
        return Decision(allowed=False, remaining=self.tokens, retry_after=retry_after)
```

(The code is abridged: the `n < 0` check and the comments are removed.)

The order is: top up, check, then either deduct or reject.

- **A rejection deducts nothing.** The tokens stay as they were.
- **`retry_after`** is computed by a formula: how many seconds it takes for the missing tokens to accumulate. If `n` is larger than `capacity`, the bucket can never hold that many, and the value is `inf`.
- **The lock** (`self._lock`) is needed because the three steps (top up, check, deduct) must run as one operation. Without it, two threads would read the same number of tokens and deduct one token twice.

## Worked example

A bucket of 5 tokens, refilling 1 token per second. The results come from running `TokenBucket` with a fake clock.

| Time, s | Request | Result | Remaining | Retry in, s |
|---|---|---|---|---|
| 0 | 1–5 | allowed | 4, 3, 2, 1, 0 | 0 |
| 0 | 6 | rejected | 0 | 1.0 |
| 0.5 | 7 | rejected | 0.5 | 0.5 |
| 1 | 8 | allowed | 0 | 0 |
| 1 | 9 | rejected | 0 | 1.0 |

The first five requests pass as a burst, because the bucket started full. After that, exactly one request per second passes.

## What surrounds the mechanism

| Layer | File | What it does |
|---|---|---|
| A bucket per client | `storage/memory.py` | Creates a `TokenBucket` for each key and calls `bucket.consume(n)` |
| HTTP | `api/middleware.py` | Takes the client key, calls the store, turns a rejection into a 429 and headers |
| Distributed variant | `storage/redis_store.py` | The same algorithm inside Redis |
| Load and visuals | `sim/`, `web/` | For demonstration only |

Dependencies point one way: `api → storage → core`. `core` knows nothing about the other layers.

## The algorithm is written in three places

1. `core/bucket.py`: Python, for the memory of one process.
2. `_CONSUME_LUA` in `storage/redis_store.py`: the same algorithm in Lua. It is a rewritten copy, not a call into the Python code: the script has to run inside Redis so that the steps "read, compute, write" are atomic.
3. `NaiveRedisStore` in the same file: an intentionally wrong version that reads, computes in Python and writes back. It exists only to show the race.

When the algorithm changes (the formula or the rejection rule), both the Python version and the Lua script have to be updated. The tests check them separately.

## How to check it

```bash
.venv/bin/python -m pytest tests/unit/test_bucket.py -v
```

The tests read like a description of the behavior, because time in them is fake and there are no sleeps:

- `test_starts_full`: a bucket is created full.
- `test_allows_burst_up_to_capacity`: a burst is limited by the bucket size.
- `test_rejected_request_does_not_spend_tokens`: a rejection deducts nothing.
- `test_refills_over_time` and `test_refill_is_fractional`: tokens come back, including in fractions.
- `test_never_exceeds_capacity`: the bucket does not overflow.
- `test_concurrent_consume_never_exceeds_capacity`: with 32 threads exactly `capacity` requests pass.
