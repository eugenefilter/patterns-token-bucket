import asyncio

import pytest
from pydantic import ValidationError

from ratelimit.sim.simulator import GroupConfig, SimConfig, Simulation
from ratelimit.storage.memory import InMemoryStore


def make_config(**overrides) -> SimConfig:
    params = {
        "capacity": 5,
        "refill_rate": 1,
        "groups": [GroupConfig(name="bot", count=2, rps=50)],
    }
    return SimConfig(**{**params, **overrides})


async def run_sim(config: SimConfig, seconds: float) -> dict:
    """Run the simulation for the given time and return the last snapshot."""
    sim = Simulation(config, InMemoryStore(config.capacity, config.refill_rate))
    sim.start()
    await asyncio.sleep(seconds)
    await sim.stop()
    return sim.snapshot()


def test_rejects_total_load_above_limit():
    with pytest.raises(ValidationError, match="total load"):
        make_config(groups=[GroupConfig(name="crowd", count=100, rps=100)])


def test_rejects_too_many_clients():
    with pytest.raises(ValidationError, match="clients in total"):
        make_config(
            groups=[GroupConfig(name="a", count=600, rps=1), GroupConfig(name="b", count=600, rps=1)]
        )


@pytest.mark.anyio
async def test_fast_clients_are_limited_to_burst_plus_refill():
    snap = await run_sim(make_config(), 0.5)  # 2 clients at 50 req/s, bucket 5, refill 1/s
    group = snap["groups"][0]
    # Each client: 5 from the bucket + at most ~1 from refill. About 10-12 in total.
    assert 10 <= group["passed"] <= 13
    assert group["rejected"] > 0


@pytest.mark.anyio
async def test_slow_client_is_never_rejected():
    config = make_config(refill_rate=5, groups=[GroupConfig(name="polite", count=1, rps=2)])
    group = (await run_sim(config, 0.6))["groups"][0]
    assert group["passed"] >= 1
    assert group["rejected"] == 0


@pytest.mark.anyio
async def test_drop_mode_never_queues_requests():
    group = (await run_sim(make_config(), 0.4))["groups"][0]
    assert group["on_limit"] == "drop"
    assert group["waiting"] <= group["count"]  # at most one request in flight per client
    # every request either passed, or got a 429 and was lost
    assert group["sent"] == group["passed"] + group["rejected"] + group["waiting"]


@pytest.mark.anyio
async def test_retry_mode_keeps_rejected_requests_in_queue():
    config = make_config(
        capacity=2,
        refill_rate=2,
        groups=[GroupConfig(name="patient", count=1, rps=20, on_limit="retry")],
    )
    group = (await run_sim(config, 0.6))["groups"][0]

    assert group["waiting"] > 0  # the client sends 20/s but ~2/s pass: the queue grows
    assert group["rejected"] > 0  # and every failed attempt got a 429
    assert group["oldest"] > 0  # the oldest request is waiting
    # requests are not lost: each one either passed or waits in the queue
    assert group["sent"] == group["passed"] + group["waiting"]


@pytest.mark.anyio
async def test_snapshot_is_frozen_after_stop():
    config = make_config()
    sim = Simulation(config, InMemoryStore(config.capacity, config.refill_rate))
    sim.start()
    await asyncio.sleep(0.1)
    await sim.stop()

    first = sim.snapshot()
    await asyncio.sleep(0.05)
    second = sim.snapshot()
    assert first["running"] is False
    assert first == second  # nothing changes after stop


@pytest.mark.anyio
async def test_snapshot_has_per_client_stats():
    config = make_config(groups=[GroupConfig(name="bots", count=3, rps=50)])
    group = (await run_sim(config, 0.3))["groups"][0]

    assert group["shown"] == len(group["clients"]) == 3
    for key in ("sent", "passed", "rejected", "waiting"):
        assert sum(c[key] for c in group["clients"]) == group[key]


@pytest.mark.anyio
async def test_only_first_clients_are_listed_for_big_groups():
    config = make_config(groups=[GroupConfig(name="crowd", count=200, rps=1)])
    group = (await run_sim(config, 0.05))["groups"][0]
    assert group["count"] == 200
    assert group["shown"] == len(group["clients"]) == 50


@pytest.mark.anyio
async def test_events_log_has_status_and_wait_info():
    config = make_config(
        capacity=1,
        refill_rate=1,
        groups=[GroupConfig(name="c", count=1, rps=30, on_limit="retry")],
    )
    events = (await run_sim(config, 0.4))["events"]

    statuses = {e["status"] for e in events}
    assert statuses == {200, 429}
    assert all("waited" in e for e in events if e["status"] == 200)
    assert all(e["retry_after"] > 0 for e in events if e["status"] == 429)
    assert [e["seq"] for e in events] == sorted(e["seq"] for e in events)


@pytest.mark.anyio
async def test_shared_key_clients_drain_one_bucket():
    # 4 clients on one key: there is one bucket (5 tokens), not four.
    config = make_config(groups=[GroupConfig(name="farm", count=4, rps=50, shared_key=True)])
    group = (await run_sim(config, 0.5))["groups"][0]

    assert group["shared"] is True
    assert 5 <= group["passed"] <= 7  # 5 from the bucket + ~0.5 from refill


@pytest.mark.parametrize("rps", [0, 0.5, 3.1])
def test_rps_must_be_a_whole_positive_number(rps):
    with pytest.raises(ValidationError):
        GroupConfig(name="x", count=1, rps=rps)
