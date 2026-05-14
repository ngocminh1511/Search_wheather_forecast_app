from __future__ import annotations

import hmac
import logging
from typing import Optional

from fastapi import Header, HTTPException, Query, status

from ..config import get_settings

log = logging.getLogger(__name__)

_HEADER_NAME = "X-Admin-Token"
_QUERY_NAME = "admin_token"


def verify_admin_token(
    x_admin_token: Optional[str] = Header(default=None, alias=_HEADER_NAME),
    admin_token: Optional[str] = Query(default=None, alias=_QUERY_NAME),
) -> None:
    """FastAPI dependency: enforces the admin token on a route.

    Behavior:
      - If `ADMIN_API_TOKEN` env is empty → auth disabled (dev/local). A WARN
        is logged once per process by `pipeline_main` lifespan.
      - Otherwise the request must present the token via header
        `X-Admin-Token: ...` OR query string `?admin_token=...` (the latter
        is needed for EventSource which can't set headers).
    """
    cfg = get_settings()
    expected = cfg.ADMIN_API_TOKEN
    if not expected:
        return  # auth disabled — explicit dev mode

    provided = x_admin_token or admin_token or ""
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid admin token",
            headers={"WWW-Authenticate": "Bearer"},
        )
