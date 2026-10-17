# Token Bucket Demo

A rate limiter on the Token Bucket algorithm, with a web lab to watch it work.

## Install

Requires Python 3.11+.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run

```bash
uvicorn ratelimit.main:app --reload
```

Open http://localhost:8000/ for the web lab. The protected endpoint is `/ping`.

To keep the state in Redis (needed for several workers):

```bash
brew services start redis        # or: docker run --rm -p 6379:6379 redis
RATELIMIT_BACKEND=redis uvicorn ratelimit.main:app --workers 4
```

With the `redis` backend the server refuses to start if Redis is unreachable.

## Configure

Set environment variables, or put them in a `.env` file (`cp .env.example .env`). Real environment variables win over the file.

| Variable | Default | Meaning |
|---|---|---|
| `RATELIMIT_CAPACITY` | `10` | Bucket size: how many requests can pass in a row |
| `RATELIMIT_REFILL_RATE` | `1` | Tokens returned per second, per client |
| `RATELIMIT_BACKEND` | `memory` | `memory` or `redis` |
| `RATELIMIT_REDIS_URL` | `redis://localhost:6379/0` | Redis address |
| `RATELIMIT_TEST_REDIS_URL` | `redis://localhost:6379/15` | Redis database for tests and `scripts/race_demo.py` |
