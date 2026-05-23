import logging
import os

import httpx
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

security = HTTPBearer()

_http_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
    return _http_client


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> dict:
    """Validate Bearer token via Supabase /auth/v1/user endpoint."""
    token = credentials.credentials
    client = _get_client()

    last_err = None
    for attempt in range(3):
        try:
            resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/user",
                headers={
                    "Authorization": f"Bearer {token}",
                    "apikey": SUPABASE_SERVICE_ROLE_KEY,
                },
            )
            break
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_err = e
            logger.warning("Supabase auth attempt %d failed: %s", attempt + 1, e)
            continue
    else:
        logger.error("Supabase auth failed after 3 attempts: %s", last_err)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Authentication service temporarily unavailable",
        )

    if resp.status_code != 200:
        logger.warning("Supabase token validation failed: %s", resp.status_code)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = resp.json()
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing user id",
        )

    return {
        "user_id": user_id,
        "email": user.get("email", ""),
        "role": user.get("role", "authenticated"),
        "token": token,
    }


async def validate_token(token: str) -> dict | None:
    """Validate a raw token string. Returns user dict or None."""
    client = _get_client()
    try:
        resp = await client.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={
                "Authorization": f"Bearer {token}",
                "apikey": SUPABASE_SERVICE_ROLE_KEY,
            },
        )
        if resp.status_code != 200:
            return None
        user = resp.json()
        user_id = user.get("id")
        if not user_id:
            return None
        return {"user_id": user_id, "email": user.get("email", "")}
    except Exception:
        return None