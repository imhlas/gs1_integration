"""Microsoft Graph API client — spec v5 sections 1 and 5.

``GraphClient`` owns all HTTP communication with the Graph API.
It implements the retry policy from the spec:

* 429  → read ``Retry-After`` header → wait → retry (max 5×)
* 401  → refresh token via AuthService → retry 1×
* 403  → raise ``PermissionError`` immediately (fatal)
* 5xx  → exponential backoff 1 s / 2 s / 4 s → max 3×, then raise

No business logic lives here — only HTTP transport + retry.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import aiohttp

from sharepoint_image_processor.core.config import AppConfig
from sharepoint_image_processor.services.auth import AuthService

logger = logging.getLogger(__name__)

_BASE_URL = "https://graph.microsoft.com/v1.0"

_DELTA_SELECT = "id,name,file,size,parentReference,listItem"
_DELTA_TOP = 1000


class GraphAPIError(Exception):
    """Non-retryable Graph API error."""

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(f"Graph API {status}: {message}")


class GraphClient:
    """Async HTTP client for Microsoft Graph with spec-compliant retry."""

    def __init__(self, auth: AuthService, config: AppConfig) -> None:
        self._auth = auth
        self._config = config
        self._session: aiohttp.ClientSession | None = None

    # ── Lifecycle ────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Core request with retry ──────────────────────────────────────

    async def _request(
        self,
        method: str,
        url: str,
        *,
        max_retries: int = 3,
        json_body: dict | None = None,
        data: bytes | None = None,
        content_type: str | None = None,
        extra_headers: dict[str, str] | None = None,
        stream: bool = False,
    ) -> aiohttp.ClientResponse:
        """Execute an HTTP request with the spec retry policy.

        Returns the *response* object.  For non-stream requests the
        caller is responsible for reading the body and closing.
        """
        session = await self._get_session()
        token = await self._auth.get_access_token()
        retried_401 = False
        retry_429_count = 0
        max_429 = 5

        for attempt in range(1, max_retries + 1):
            headers: dict[str, str] = {"Authorization": f"Bearer {token}"}
            if content_type:
                headers["Content-Type"] = content_type
            if extra_headers:
                headers.update(extra_headers)

            kwargs: dict[str, Any] = {"headers": headers}
            if json_body is not None:
                kwargs["json"] = json_body
            elif data is not None:
                kwargs["data"] = data

            resp = await session.request(method, url, **kwargs)

            # ── 2xx: success ─────────────────────────────────────
            if 200 <= resp.status < 300:
                return resp

            body_text = await resp.text()

            # ── 429: throttled ───────────────────────────────────
            if resp.status == 429:
                retry_429_count += 1
                if retry_429_count > max_429:
                    raise GraphAPIError(429, f"Throttled {max_429}× — giving up")
                retry_after = int(resp.headers.get("Retry-After", "5"))
                logger.warning("429 throttled — waiting %ds (attempt %d/%d)", retry_after, retry_429_count, max_429)
                await asyncio.sleep(retry_after)
                continue

            # ── 401: token expired ───────────────────────────────
            if resp.status == 401 and not retried_401:
                retried_401 = True
                logger.warning("401 — refreshing token and retrying")
                token = await self._auth.force_refresh()
                continue

            # ── 403: permission denied (fatal) ───────────────────
            if resp.status == 403:
                raise PermissionError(f"Graph API 403 Forbidden: {body_text[:300]}")

            # ── 5xx: server error ────────────────────────────────
            if resp.status >= 500:
                if attempt < max_retries:
                    wait = 2 ** (attempt - 1)  # 1, 2, 4
                    logger.warning("Graph %d — backoff %ds (attempt %d/%d)", resp.status, wait, attempt, max_retries)
                    await asyncio.sleep(wait)
                    continue
                raise GraphAPIError(resp.status, f"Server error after {max_retries} retries: {body_text[:300]}")

            # ── Other errors ─────────────────────────────────────
            raise GraphAPIError(resp.status, body_text[:500])

        # Should not reach here, but guard against it
        raise GraphAPIError(0, "Exhausted retries without a response")

    async def _get_json(
        self, url: str, *, max_retries: int = 3, extra_headers: dict[str, str] | None = None
    ) -> dict:
        resp = await self._request("GET", url, max_retries=max_retries, extra_headers=extra_headers)
        try:
            return await resp.json()
        finally:
            resp.close()

    # ── Public API (spec section 5) ──────────────────────────────────

    async def resolve_site_id(self, site_url: str) -> str:
        """GET /sites/{host}:/sites/{path} → site.id"""
        # site_url expected as "lejosfi.sharepoint.com:/sites/Insights"
        url = f"{_BASE_URL}/sites/{site_url}"
        data = await self._get_json(url)
        site_id = data.get("id")
        if not site_id:
            raise GraphAPIError(0, f"Site resolution returned no id: {data}")
        logger.info("Resolved site '%s' → %s", data.get("displayName", "?"), site_id)
        return site_id

    async def resolve_drive_id(self, site_id: str, library_name: str) -> str:
        """GET /sites/{siteId}/drives → find drive by name → drive.id"""
        url = f"{_BASE_URL}/sites/{site_id}/drives"
        data = await self._get_json(url)
        for drive in data.get("value", []):
            if drive.get("name") == library_name:
                drive_id = drive["id"]
                logger.info("Resolved drive '%s' → %s", library_name, drive_id)
                return drive_id
        available = [d.get("name") for d in data.get("value", [])]
        raise GraphAPIError(0, f"Drive '{library_name}' not found. Available: {available}")

    async def list_delta_items(
        self, drive_id: str, delta_token: str | None = None
    ) -> tuple[list[dict], str]:
        """GET delta listing — returns (items, new_delta_link).

        Paginates automatically through ``@odata.nextLink``.
        """
        if delta_token:
            url = delta_token
        else:
            url = (
                f"{_BASE_URL}/drives/{drive_id}/root/delta"
                f"?$select={_DELTA_SELECT}&$top={_DELTA_TOP}"
            )

        all_items: list[dict] = []

        while url:
            data = await self._get_json(url)
            all_items.extend(data.get("value", []))
            url = data.get("@odata.nextLink")  # type: ignore[assignment]

        delta_link = data.get("@odata.deltaLink", "")
        logger.info("Delta listing returned %d items", len(all_items))
        return all_items, delta_link

    async def list_items_filtered(
        self, drive_id: str, filters: list[tuple[str, str]]
    ) -> list[dict]:
        """List drive items using server-side $filter on listItem fields.

        Much faster than delta + client-side filtering because SharePoint
        returns only matching items.  Paginates via @odata.nextLink.

        *filters* is a list of (field_name, value) tuples combined with AND.
        """
        filter_parts = [
            f"fields/{key} eq '{value}'" for key, value in filters
        ]
        filter_expr = " and ".join(filter_parts)

        url = (
            f"{_BASE_URL}/drives/{drive_id}/list/items"
            f"?$expand=driveItem($select=id,name,file,size,parentReference),fields"
            f"&$filter={filter_expr}"
            f"&$top=200"
        )

        # Non-indexed columns require this header
        prefer_header = {"Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly"}

        all_items: list[dict] = []
        while url:
            data = await self._get_json(url, extra_headers=prefer_header)
            for item in data.get("value", []):
                # Reshape to match delta response format so DriveItem.from_graph_response works
                drive_item = item.get("driveItem", {})
                if drive_item:
                    drive_item["listItem"] = {"fields": item.get("fields", {})}
                    all_items.append(drive_item)
            url = data.get("@odata.nextLink")

        logger.info("Filtered listing returned %d items", len(all_items))
        return all_items

    async def download_item(self, drive_id: str, item_id: str, dest_path: Path) -> Path:
        """GET /items/{id}/content → save to *dest_path* → return path."""
        url = f"{_BASE_URL}/drives/{drive_id}/items/{item_id}/content"
        resp = await self._request("GET", url, stream=True)
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(await resp.read())
        finally:
            resp.close()
        logger.debug("Downloaded %s → %s", item_id, dest_path)
        return dest_path

    async def upload_file(
        self, drive_id: str, parent_path: str, filename: str, data: bytes
    ) -> dict:
        """PUT /root:/{parentPath}/{filename}:/content → return new item dict.

        The spec uses simple PUT upload (< 4 MB boundary handled by Graph).
        For files > 4 MB an upload session would be needed — this is not
        currently implemented (see spec gap note in the earlier review).
        """
        # parent_path from Graph looks like "/drives/xyz/root:/Juomat"
        # We need just the path after "root:" for the upload URL.
        rel_path = parent_path
        root_marker = "/root:"
        idx = rel_path.find(root_marker)
        if idx != -1:
            rel_path = rel_path[idx + len(root_marker):]
        # Ensure leading slash
        if rel_path and not rel_path.startswith("/"):
            rel_path = "/" + rel_path

        url = f"{_BASE_URL}/drives/{drive_id}/root:{rel_path}/{filename}:/content?@microsoft.graph.conflictBehavior=replace"
        resp = await self._request(
            "PUT", url, data=data, content_type="image/png"
        )
        try:
            result = await resp.json()
        finally:
            resp.close()
        logger.info("Uploaded %s → item %s", filename, result.get("id"))
        return result

    async def get_item_fields(self, drive_id: str, item_id: str) -> dict:
        """GET /items/{id}/listItem/fields → return fields dict."""
        url = f"{_BASE_URL}/drives/{drive_id}/items/{item_id}/listItem/fields"
        return await self._get_json(url)

    async def patch_item_fields(self, drive_id: str, item_id: str, fields: dict) -> dict:
        """PATCH /items/{id}/listItem/fields → return updated fields."""
        url = f"{_BASE_URL}/drives/{drive_id}/items/{item_id}/listItem/fields"
        resp = await self._request("PATCH", url, json_body=fields)
        try:
            return await resp.json()
        finally:
            resp.close()

    async def get_item(self, drive_id: str, item_id: str) -> dict:
        """GET /items/{id} → return full item dict (verify operation, 1× retry)."""
        url = f"{_BASE_URL}/drives/{drive_id}/items/{item_id}?$select=id,name,size,file,listItem"
        return await self._get_json(url, max_retries=1)

    async def delete_item(self, drive_id: str, item_id: str) -> bool:
        """DELETE /items/{id} → return True on 204, False on failure."""
        url = f"{_BASE_URL}/drives/{drive_id}/items/{item_id}"
        try:
            resp = await self._request("DELETE", url)
            resp.close()
            logger.info("Deleted item %s", item_id)
            return True
        except (GraphAPIError, Exception) as exc:
            logger.warning("Failed to delete item %s: %s", item_id, exc)
            return False
