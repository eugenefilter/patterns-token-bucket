import pytest
from fastapi.testclient import TestClient

from ratelimit.api.app import create_app
from ratelimit.storage.memory import InMemoryStore


@pytest.fixture
def client(clock) -> TestClient:
    store = InMemoryStore(capacity=3, refill_rate=1, clock=clock)
    return TestClient(create_app(store))


def test_allows_burst_then_returns_429(client):
    statuses = [client.get("/ping").status_code for _ in range(4)]
    assert statuses == [200, 200, 200, 429]


def test_rate_limit_headers_on_success(client):
    response = client.get("/ping")
    assert response.headers["X-RateLimit-Limit"] == "3"
    assert response.headers["X-RateLimit-Remaining"] == "2"
    assert "Retry-After" not in response.headers


def test_429_has_retry_after(client):
    for _ in range(3):
        client.get("/ping")
    response = client.get("/ping")
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "1"
    assert response.headers["X-RateLimit-Remaining"] == "0"


def test_recovers_after_waiting(client, clock):
    for _ in range(4):
        client.get("/ping")
    clock.advance(1)
    assert client.get("/ping").status_code == 200


def test_api_keys_are_limited_independently(client):
    for _ in range(3):
        client.get("/ping", headers={"X-API-Key": "alice"})
    assert client.get("/ping", headers={"X-API-Key": "alice"}).status_code == 429
    assert client.get("/ping", headers={"X-API-Key": "bob"}).status_code == 200
