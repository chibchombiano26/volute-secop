"""Auth por API key compartida (REST + MCP SSE).

Env API_KEYS="key1,key2" (coma). Rutas abiertas: /health.
Clientes: header X-API-Key o ?api_key=.
"""
import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

OPEN = {"/health", "/openapi.json", "/scalar"}


def valid(key: str) -> bool:
    keys = [k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()]
    if not keys:
        return True  # sin configurar: abierto (desarrollo)
    return bool(key) and key in keys


class ApiKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if request.url.path in OPEN:
            return await call_next(request)
        key = request.headers.get("x-api-key") or request.query_params.get("api_key")
        if not valid(key):
            return JSONResponse({"detail": "api key invalida o ausente"}, 401)
        return await call_next(request)
