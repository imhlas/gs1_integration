"""Transactional upload, metadata copy, verify, delete, and rollback.

Spec v5 section 5 + E2E workflow (execute_transaction detail).

``UploadAgent`` owns:
- Upload processed PNG to SharePoint
- Copy metadata (whitelist → required check → PATCH)
- Verify uploaded item (size > 0, mimeType == image/png)
- Delete old source item
- Rollback on any failure (delete the newly uploaded PNG)
- Cleanup orphan PNGs

It does NOT do image processing.
"""

from __future__ import annotations

import logging

from sharepoint_image_processor.core.config import AppConfig
from sharepoint_image_processor.core.metadata import (
    build_patch_payload,
    extract_copyable_fields,
    validate_required_fields,
)
from sharepoint_image_processor.core.models import (
    DriveItem,
    ItemClassification,
    ProcessingResult,
    ProcessingStatus,
)
from sharepoint_image_processor.services.graph_client import GraphClient

logger = logging.getLogger(__name__)


class UploadAgent:
    """Manages the transactional upload lifecycle for processed images."""

    def __init__(self, graph: GraphClient, config: AppConfig) -> None:
        self._graph = graph
        self._config = config

    # ── Individual operations ────────────────────────────────────────

    async def upload_processed_image(
        self, drive_id: str, original: DriveItem, png_data: bytes
    ) -> str:
        """PUT new PNG → return new item ID."""
        filename = f"{original.stem}.png"
        new_item = await self._graph.upload_file(
            drive_id, original.parent_path, filename, png_data
        )
        return new_item["id"]

    async def copy_metadata(
        self, drive_id: str, old_item_id: str, new_item_id: str
    ) -> bool:
        """Read old fields → whitelist → required check → PATCH new item.

        Returns ``True`` on success, raises on validation failure.
        """
        old_fields = await self._graph.get_item_fields(drive_id, old_item_id)
        copyable = extract_copyable_fields(old_fields)
        payload = build_patch_payload(copyable, old_item_id)

        ok, missing = validate_required_fields(payload)
        if not ok:
            raise ValueError(f"Missing required metadata fields: {missing}")

        await self._graph.patch_item_fields(drive_id, new_item_id, payload)
        return True

    async def verify_uploaded_item(self, drive_id: str, item_id: str) -> bool:
        """GET item → verify size > 0 and mimeType == image/png."""
        item_data = await self._graph.get_item(drive_id, item_id)
        size = item_data.get("size", 0)
        mime = item_data.get("file", {}).get("mimeType", "")
        if size == 0:
            logger.warning("Verify failed: item %s has size 0", item_id)
            return False
        if mime != "image/png":
            logger.warning("Verify failed: item %s has mimeType '%s'", item_id, mime)
            return False
        return True

    async def delete_old_item(self, drive_id: str, item_id: str) -> bool:
        """DELETE old source item → return success."""
        return await self._graph.delete_item(drive_id, item_id)

    async def cleanup_orphan(self, drive_id: str, orphan_item_id: str) -> bool:
        """Delete an orphan PNG that was left from a previous incomplete run."""
        logger.info("Cleaning up orphan PNG: %s", orphan_item_id)
        return await self._graph.delete_item(drive_id, orphan_item_id)

    # ── Full transaction ─────────────────────────────────────────────

    async def execute_transaction(
        self, drive_id: str, original: DriveItem, png_data: bytes
    ) -> ProcessingResult:
        """Run the complete upload transaction with rollback on failure.

        Steps:
        1. Upload PNG
        2. Copy metadata (whitelist + required check)
        3. Verify uploaded item
        4. Delete old source

        On failure at any step, the newly uploaded PNG is deleted
        (rollback) and a FAILED result is returned.
        """
        new_item_id: str | None = None

        try:
            # Step 1: Upload
            new_item_id = await self.upload_processed_image(
                drive_id, original, png_data
            )
            new_item_name = f"{original.stem}.png"

            # Step 2: Copy metadata
            try:
                await self.copy_metadata(drive_id, original.id, new_item_id)
            except ValueError as exc:
                # Missing required fields → rollback
                logger.warning("Metadata validation failed for %s: %s", original.name, exc)
                await self._graph.delete_item(drive_id, new_item_id)
                return ProcessingResult(
                    item_id=original.id,
                    item_name=original.name,
                    status=ProcessingStatus.FAILED,
                    classification=ItemClassification.PROCESS,
                    error_message=str(exc),
                )

            # Step 3: Verify
            verified = await self.verify_uploaded_item(drive_id, new_item_id)
            if not verified:
                await self._graph.delete_item(drive_id, new_item_id)
                return ProcessingResult(
                    item_id=original.id,
                    item_name=original.name,
                    status=ProcessingStatus.FAILED,
                    classification=ItemClassification.PROCESS,
                    error_message="verify_failed_empty",
                )

            # Step 4: Delete old source (skip if upload replaced in-place,
            # e.g. PNG→PNG where the new item has the same ID)
            if new_item_id != original.id:
                deleted = await self.delete_old_item(drive_id, original.id)
                if not deleted:
                    logger.warning(
                        "Could not delete old item %s — both old and new exist",
                        original.id,
                    )
            else:
                logger.info("Source replaced in-place (same name): %s", original.name)

            return ProcessingResult(
                item_id=original.id,
                item_name=original.name,
                status=ProcessingStatus.SUCCESS,
                classification=ItemClassification.PROCESS,
                new_item_id=new_item_id,
                new_item_name=new_item_name,
            )

        except Exception as exc:
            # Rollback: delete the newly uploaded PNG if it exists
            if new_item_id:
                logger.warning("Rolling back upload for %s", original.name)
                await self._graph.delete_item(drive_id, new_item_id)

            return ProcessingResult(
                item_id=original.id,
                item_name=original.name,
                status=ProcessingStatus.FAILED,
                classification=ItemClassification.PROCESS,
                error_message=str(exc),
            )
