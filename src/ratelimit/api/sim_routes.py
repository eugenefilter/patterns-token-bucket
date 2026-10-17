"""Simulator routes: start, stop and the state stream (SSE)."""

import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from ratelimit.sim.simulator import SimConfig, SimulationManager
from ratelimit.storage.base import StoreUnavailableError

TICK_SECONDS = 0.1  # how often the page receives a fresh state


def create_sim_router(manager: SimulationManager) -> APIRouter:
    router = APIRouter(prefix="/sim", tags=["simulation"])

    @router.post("/start")
    async def start(config: SimConfig) -> dict[str, bool]:
        try:
            await manager.start(config)
        except StoreUnavailableError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return {"ok": True}

    @router.post("/stop")
    async def stop() -> dict[str, bool]:
        await manager.stop()
        return {"ok": True}

    @router.get("/stream")
    async def stream() -> StreamingResponse:
        async def events() -> AsyncIterator[str]:
            # SSE format: a "data: <json>" line followed by an empty line.
            # When the browser closes the tab, Starlette cancels this generator.
            while True:
                sim = manager.current
                payload = sim.snapshot() if sim else {"running": False, "groups": []}
                yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(TICK_SECONDS if sim and sim.running else 0.5)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return router
