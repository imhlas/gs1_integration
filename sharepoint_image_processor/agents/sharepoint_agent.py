"""SharePoint listing, filtering, classification, and batch download.

Spec v5 section 5 — ``SharePointAgent`` owns:
- Delta query → filtering → classify
- Batch download to temp directory

It does NOT do image processing or HTTP retries (those belong to
``image_service`` and ``graph_client`` respectively).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Sequence

from sharepoint_image_processor.core.config import AppConfig
from sharepoint_image_processor.core.models import (
    DriveItem,
    ItemClassification,
)
from sharepoint_image_processor.services.graph_client import GraphClient

logger = logging.getLogger(__name__)

# MIME types we accept for processing
_PROCESSABLE_MIMES = frozenset({
    "image/jpeg",
    "image/tiff",
    "image/png",
})


class SharePointAgent:
    """Lists, filters, classifies, and downloads SharePoint drive items."""

    def __init__(self, graph: GraphClient, config: AppConfig) -> None:
        self._graph = graph
        self._config = config

    # ── Public API ───────────────────────────────────────────────────

    async def list_and_classify(
        self,
        drive_id: str,
        delta_token: str | None,
        filters: list[str],
    ) -> tuple[list[tuple[DriveItem, ItemClassification]], str]:
        """Run delta query → filter → classify every item.

        Returns ``(classified_items, new_delta_link)`` where each entry
        in *classified_items* is a ``(DriveItem, ItemClassification)`` pair.
        """
        # When filters are given, use server-side filtering (fast) instead
        # of delta + client-side enrichment (slow: 1 API call per item).
        if filters:
            parsed_filters = []
            for f in filters:
                if "=" in f:
                    key, value = f.split("=", 1)
                    parsed_filters.append((key.strip(), value.strip()))

            raw_items = await self._graph.list_items_filtered(drive_id, parsed_filters)
            new_delta_link = ""  # No delta token from filtered listing
            logger.info("Filtered listing returned %d raw items", len(raw_items))
        else:
            raw_items, new_delta_link = await self._graph.list_delta_items(
                drive_id, delta_token
            )
            logger.info("Delta returned %d raw items", len(raw_items))

        # Build sibling index (items grouped by parent folder) for
        # duplicate / orphan detection during classification.
        siblings_by_parent: dict[str, list[DriveItem]] = {}
        drive_items: list[DriveItem] = []

        for raw in raw_items:
            # Skip entries that have no "file" key (folders, deleted refs)
            if "file" not in raw and "folder" not in raw:
                continue
            item = DriveItem.from_graph_response(raw)
            drive_items.append(item)
            siblings_by_parent.setdefault(item.parent_id, []).append(item)

        # Classify
        classified: list[tuple[DriveItem, ItemClassification]] = []
        for item in drive_items:
            siblings = siblings_by_parent.get(item.parent_id, [])
            cls = self.classify_item(item, siblings)
            classified.append((item, cls))

        counts = _count_classifications(classified)
        logger.info("Classification summary: %s", counts)
        return classified, new_delta_link

    async def download_batch(
        self,
        drive_id: str,
        items: Sequence[tuple[DriveItem, ItemClassification]],
        dest_dir: str,
        graph_sem: asyncio.Semaphore,
    ) -> list[tuple[DriveItem, Path]]:
        """Download processable items in *items* to *dest_dir* concurrently.

        Concurrency is bounded by *graph_sem*.
        Returns list of ``(DriveItem, local_path)`` pairs for successfully
        downloaded items.
        """
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)

        async def _dl(item: DriveItem) -> tuple[DriveItem, Path] | None:
            async with graph_sem:
                try:
                    local = dest / f"{item.id}_{item.name}"
                    await self._graph.download_item(drive_id, item.id, local)
                    return (item, local)
                except Exception as exc:
                    logger.warning("Download failed for %s: %s", item.name, exc)
                    return None

        tasks = [_dl(item) for item, cls in items if cls == ItemClassification.PROCESS]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [r for r in results if r is not None]

    # ── Filtering ────────────────────────────────────────────────────

    @staticmethod
    def should_process(item: DriveItem) -> tuple[bool, str]:
        """Pre-classification gate: MIME, size, folder check.

        Returns ``(True, "")`` if the item passes, or
        ``(False, reason_string)`` explaining why it should be skipped.
        """
        if not item.mime_type:
            return False, "no_mime_type"
        if item.mime_type not in _PROCESSABLE_MIMES:
            return False, f"unsupported_mime:{item.mime_type}"
        if item.size <= 0:
            return False, "zero_size"
        return True, ""

    async def _enrich_with_fields(
        self, drive_id: str, items: list[DriveItem], filter_keys: list[str] | None = None
    ) -> None:
        """Fetch ``listItem.fields`` for each item concurrently.

        Delta endpoint returns partial fields (system columns only), so
        custom columns like EAN/BRAND are missing.  When *filter_keys*
        is given, any item missing those keys in its fields dict is
        enriched.  Otherwise, only items with completely empty fields
        are enriched.  Progress is logged every 100 items.
        """
        if filter_keys:
            # Delta returns system fields but not custom ones — re-fetch
            # for any item that is missing at least one filter key
            items_needing_fields = [
                i for i in items
                if i.mime_type and not all(k in i.fields for k in filter_keys)
            ]
        else:
            items_needing_fields = [i for i in items if not i.fields and i.mime_type]
        if not items_needing_fields:
            return

        total = len(items_needing_fields)
        logger.info("Enriching %d items with metadata fields...", total)
        sem = asyncio.Semaphore(self._config.graph_api_concurrency)
        done_count = 0

        async def _fetch(item: DriveItem) -> None:
            nonlocal done_count
            async with sem:
                try:
                    fields = await self._graph.get_item_fields(drive_id, item.id)
                    item.fields = fields
                except Exception as exc:
                    logger.debug("Could not fetch fields for %s: %s", item.name, exc)
                done_count += 1
                if done_count % 100 == 0 or done_count == total:
                    logger.info("Fields enrichment progress: %d/%d", done_count, total)

        await asyncio.gather(*[_fetch(item) for item in items_needing_fields])
        logger.info("Fields enrichment complete")

    def _apply_filters(
        self, items: list[DriveItem], filters: list[str]
    ) -> list[DriveItem]:
        """Apply KEY=VALUE metadata filters. All filters must match (AND)."""
        parsed: list[tuple[str, str]] = []
        for f in filters:
            if "=" not in f:
                logger.warning("Ignoring malformed filter: %s", f)
                continue
            key, value = f.split("=", 1)
            parsed.append((key.strip(), value.strip()))

        if not parsed:
            return items

        filtered: list[DriveItem] = []
        for item in items:
            if all(
                item.fields.get(key) == value for key, value in parsed
            ):
                filtered.append(item)

        logger.info(
            "Filters %s reduced %d → %d items",
            filters, len(items), len(filtered),
        )
        return filtered

    # ── Classification (idempotency logic) ───────────────────────────

    def classify_item(
        self, item: DriveItem, siblings: list[DriveItem]
    ) -> ItemClassification:
        """Determine what to do with *item* based on its properties and siblings.

        Implements the idempotency rules from the spec:
        - Folder → SKIP_FOLDER
        - Already processed PNG (Processed==True) → SKIP_PROCESSED
        - Manual PNG without Processed flag → SKIP_MANUAL_PNG
        - Unsupported MIME → SKIP_UNSUPPORTED
        - Too small / too large → SKIP_TOO_SMALL / SKIP_TOO_LARGE
        - Source JPG/TIFF with a sibling .png of the same stem and
          Processed==True on the PNG → CLEANUP_DELETE_OLD (source is stale)
        - Orphan PNG (Processed==True but no matching source) → CLEANUP_DELETE_ORPHAN
        - Source with a sibling manual PNG (no Processed flag) → QUARANTINE_DUPLICATE
        - Otherwise → PROCESS
        """
        # Folder
        if not item.mime_type:
            return ItemClassification.SKIP_FOLDER

        # Already a processed PNG (our output) — skip
        if item.extension == ".png" and item.fields.get("Processed") is True:
            return ItemClassification.SKIP_PROCESSED

        # PNG without Processed flag falls through to PROCESS
        # (may have white/solid background that needs removal)

        # MIME check
        ok, _reason = self.should_process(item)
        if not ok:
            return ItemClassification.SKIP_UNSUPPORTED

        # Size checks
        max_bytes = self._config.max_image_size_mb * 1024 * 1024
        if item.size > max_bytes:
            return ItemClassification.SKIP_TOO_LARGE
        # min_image_size_px is a pixel check; at listing time we only have
        # byte size, so we use a conservative heuristic: if the file is
        # smaller than ~1 KB it is almost certainly too small.
        if item.size < 1024:
            return ItemClassification.SKIP_TOO_SMALL

        # Sibling analysis
        sibling_names = {s.name: s for s in siblings if s.id != item.id}
        expected_png_name = f"{item.stem}.png"
        sibling_png = sibling_names.get(expected_png_name)

        if sibling_png is not None:
            if sibling_png.fields.get("Processed") is True:
                # Source still present after successful processing → it is
                # the old item that should have been deleted.
                return ItemClassification.CLEANUP_DELETE_OLD
            else:
                # A manual PNG with the same stem exists → quarantine
                return ItemClassification.QUARANTINE_DUPLICATE

        return ItemClassification.PROCESS


def _count_classifications(
    items: list[tuple[DriveItem, ItemClassification]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for _, cls in items:
        counts[cls.value] = counts.get(cls.value, 0) + 1
    return counts
