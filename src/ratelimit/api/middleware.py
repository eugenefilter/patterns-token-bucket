"""Middleware: deducts a token on every request and answers 429 on rejection.

Written as pure ASGI rather than through BaseHTTPMiddleware: that one wraps the response
and is awkward for streaming responses (SSE), which the simulator needs.
"""

import math

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ratelimit.api.keys import client_key
from ratelimit.storage.base import RateLimitStore

# The page and the simulator must not spend the main limiter's tokens.
DEFAULT_EXEMPT_PREFIXES = ("/sim", "/docs", "/openapi.json")


class RateLimitMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        store: RateLimitStore,
        exempt_prefixes: tuple[str, ...] = DEFAULT_EXEMPT_PREFIXES,
    ) -> None:
        self.app = app
        self.store = store
        self.exempt_prefixes = exempt_prefixes

    def _is_exempt(self, path: str) -> bool:
        return path == "/" or path.startswith(self.exempt_prefixes)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or self._is_exempt(scope["path"]):
            await self.app(scope, receive, send)
            return

        decision = await self.store.consume(client_key(Request(scope)))
        headers = {
            "X-RateLimit-Limit": str(int(self.store.capacity)),
            "X-RateLimit-Remaining": str(int(decision.remaining)),
        }

        if not decision.allowed:
            # Retry-After per the HTTP standard: whole seconds, rounded up.
            headers["Retry-After"] = str(max(1, math.ceil(decision.retry_after)))
            response = JSONResponse({"detail": "Too Many Requests"}, status_code=429, headers=headers)
            await response(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
                message = {**message, "headers": [*message.get("headers", []), *raw]}
            await send(message)

        await self.app(scope, receive, send_with_headers)
