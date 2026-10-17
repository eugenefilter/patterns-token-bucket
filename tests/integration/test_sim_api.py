import pytest
from fastapi.testclient import TestClient

from ratelimit.api.app import create_app
from ratelimit.sim.simulator import SimulationManager
from ratelimit.storage.memory import InMemoryStore

VALID_CONFIG = {
    "capacity": 5,
    "refill_rate": 1,
    "backend": "memory",
    "groups": [{"name": "bot", "count": 1, "rps": 10}],
}


@pytest.fixture
def client(clock):
    store = InMemoryStore(capacity=3, refill_rate=1, clock=clock)
    manager = SimulationManager(lambda cfg: InMemoryStore(cfg.capacity, cfg.refill_rate))
    with TestClient(create_app(store, manager)) as test_client:  # with: runs the lifespan
        yield test_client


def test_start_and_stop(client):
    assert client.post("/sim/start", json=VALID_CONFIG).json() == {"ok": True}
    assert client.post("/sim/stop").json() == {"ok": True}


def test_invalid_config_returns_422(client):
    bad = {**VALID_CONFIG, "capacity": -1}
    assert client.post("/sim/start", json=bad).status_code == 422


def test_index_page_is_served(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_simulator_endpoints_do_not_spend_main_limiter_tokens(client):
    # The main limiter's capacity is 3, but service routes do not spend it.
    statuses = {client.post("/sim/stop").status_code for _ in range(20)}
    assert statuses == {200}
    assert client.get("/ping").status_code == 200
