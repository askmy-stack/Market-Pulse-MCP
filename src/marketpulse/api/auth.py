"""API key authentication middleware."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from marketpulse.config import get_settings


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Require X-API-Key header when API_KEY env var is set."""

    EXEMPT_PATHS = {"/health", "/metrics", "/docs", "/openapi.json", "/redoc"}

    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        if not settings.api_key:
            return await call_next(request)

        path = request.url.path
        if path in self.EXEMPT_PATHS or path.startswith("/docs"):
            return await call_next(request)

        provided = request.headers.get("X-API-Key")
        if provided != settings.api_key:
            return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key"})

        return await call_next(request)
