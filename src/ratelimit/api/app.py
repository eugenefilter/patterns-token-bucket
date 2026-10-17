"""Assembling the FastAPI application. The store and the simulator come from outside."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from ratelimit.api.middleware import RateLimitMiddleware
from ratelimit.api.sim_routes import create_sim_router
from ratelimit.sim.simulator import SimulationManager
from ratelimit.storage.base import RateLimitStore

INDEX_HTML = Path(__file__).resolve().parent.parent / "web" / "index.html"


def create_app(store: RateLimitStore, sim_manager: SimulationManager | None = None) -> FastAPI:
    """Factory: tests plug in their own store with a fake clock."""

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await store.ping()  # fail fast: do not start if the store is unreachable
        yield
        if sim_manager:
            await sim_manager.stop()  # do not leave clients running after shutdown

    app = FastAPI(title="Token Bucket Demo", lifespan=lifespan)
    app.add_middleware(RateLimitMiddleware, store=store)

    @app.get("/ping")
    async def ping() -> dict[str, str]:
        return {"message": "pong"}

    if sim_manager:
        app.include_router(create_sim_router(sim_manager))

        @app.get("/", include_in_schema=False)
        async def index() -> FileResponse:
            return FileResponse(INDEX_HTML)

    return app
