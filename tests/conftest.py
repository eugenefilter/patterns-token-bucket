import pytest


class FakeClock:
    """Time controlled by the test: no sleeps."""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def anyio_backend() -> str:
    """Async tests (@pytest.mark.anyio) run on asyncio only."""
    return "asyncio"
