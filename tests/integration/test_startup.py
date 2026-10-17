import pytest
from fastapi.testclient import TestClient

from ratelimit.api.app import create_app
from ratelimit.sim.simulator import SimulationManager
from ratelimit.storage.base import StoreUnavailableError
from ratelimit.storage.memory import InMemoryStore

VALID_SIM_CONFIG = {
    "capacity": 5,
    "refill_rate": 1,
    "backend": "memory",
    "groups": [{"name": "bot", "count": 1, "rps": 10}],
}


class DownStore(InMemoryStore):
    """A store that cannot be reached, like Redis that is not running."""

    async def ping(self) -> None:
        raise StoreUnavailableError("Redis is unavailable: connection refused")


def test_app_refuses_to_start_when_store_is_unreachable():
    app = create_app(DownStore(capacity=1, refill_rate=1))
    with pytest.raises(StoreUnavailableError, match="connection refused"):
        with TestClient(app):
            pass


def test_simulation_start_returns_503_when_its_store_is_unreachable(clock):
    manager = SimulationManager(lambda cfg: DownStore(cfg.capacity, cfg.refill_rate))
    app = create_app(InMemoryStore(capacity=3, refill_rate=1, clock=clock), manager)
    with TestClient(app) as client:
        response = client.post("/sim/start", json=VALID_SIM_CONFIG)
    assert response.status_code == 503
    assert "connection refused" in response.json()["detail"]
