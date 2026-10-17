"""How to tell who is making a request."""

from fastapi import Request


def client_key(request: Request) -> str:
    """Who this client is: by API key, or by IP when there is none.

    Behind a proxy (nginx, load balancer) request.client.host is the proxy address,
    and all clients collapse into one. A trusted X-Forwarded-For is needed there.
    """
    api_key = request.headers.get("x-api-key")
    if api_key:
        return f"key:{api_key}"
    host = request.client.host if request.client else "unknown"
    return f"ip:{host}"
