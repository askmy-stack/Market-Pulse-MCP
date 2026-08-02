"""Rate limiting helpers for MarketPulse API."""

from __future__ import annotations

import os

from slowapi import Limiter
from slowapi.util import get_remote_address

# Hard cap for Query(limit=...) across list endpoints.
MAX_QUERY_LIMIT = 500


def api_rate_limit(*_args, **_kwargs) -> str:
    return (os.environ.get("MARKETPULSE_RATE_LIMIT", "60/minute").strip() or "60/minute")


limiter = Limiter(key_func=get_remote_address, headers_enabled=True)
