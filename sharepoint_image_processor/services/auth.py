"""MSAL client-credentials authentication — spec v5 section 5.

``AuthService`` is the sole owner of token acquisition and caching.
Other modules never touch MSAL directly.
"""

from __future__ import annotations

import logging
import time

import msal

from sharepoint_image_processor.core.config import AppConfig

logger = logging.getLogger(__name__)

_GRAPH_SCOPE = "https://graph.microsoft.com/.default"

# Refresh token 5 minutes before expiry
_EXPIRY_BUFFER_S = 300


class AuthService:
    """Acquires and caches an Azure AD access token via MSAL client credentials."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._app = msal.ConfidentialClientApplication(
            client_id=config.azure_client_id,
            client_credential=config.azure_client_secret,
            authority=f"https://login.microsoftonline.com/{config.azure_tenant_id}",
        )
        self._cached_token: str | None = None
        self._token_expiry: float = 0.0

    async def get_access_token(self) -> str:
        """Return a valid access token, using an in-memory cache.

        Only acquires a new token when the cached one is expired or
        about to expire (within 5 minutes).

        Raises ``RuntimeError`` on authentication failure.
        """
        if self._cached_token and time.monotonic() < self._token_expiry:
            return self._cached_token

        return await self._acquire_new_token()

    async def force_refresh(self) -> str:
        """Force a fresh token acquisition (called on 401 retry)."""
        logger.info("Forcing token refresh after 401")
        self._cached_token = None
        self._token_expiry = 0.0
        return await self._acquire_new_token()

    async def _acquire_new_token(self) -> str:
        """Acquire a fresh client-credentials token from MSAL."""
        logger.info("Acquiring new client-credentials token")
        result = self._app.acquire_token_for_client(scopes=[_GRAPH_SCOPE])

        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "unknown"))
            raise RuntimeError(f"MSAL authentication failed: {error}")

        self._cached_token = result["access_token"]
        # expires_in is typically 3600 seconds
        expires_in = result.get("expires_in", 3600)
        self._token_expiry = time.monotonic() + expires_in - _EXPIRY_BUFFER_S
        return self._cached_token
