"""State and delta-token persistence — spec v5 section 3.

Reads / writes ``state.json`` and ``delta_token.json``.  Paths may be
local filesystem paths or ``dbfs:/…`` paths.  On Databricks the DBFS
FUSE mount translates ``dbfs:/`` → ``/dbfs/``, so we normalise paths
once in ``__init__``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sharepoint_image_processor.core.models import (
    DriveItem,
    ProcessingResult,
    ProcessingStatus,
)

logger = logging.getLogger(__name__)

_STATE_VERSION = 1
_DELTA_TOKEN_VERSION = 1


def _normalise_path(raw: str) -> Path:
    """Translate ``dbfs:/…`` to the FUSE mount ``/dbfs/…`` and return a Path."""
    if raw.startswith("dbfs:"):
        return Path("/dbfs" + raw[5:])
    return Path(raw)


class StateManager:
    """Manages ``state.json`` and ``delta_token.json`` on disk / DBFS."""

    def __init__(self, state_path: str, delta_token_path: str) -> None:
        self._state_path = _normalise_path(state_path)
        self._delta_token_path = _normalise_path(delta_token_path)
        self._state: dict[str, Any] | None = None

    # ── state.json ───────────────────────────────────────────────────

    def load_state(self) -> dict[str, Any] | None:
        """Load ``state.json``.  Returns ``None`` when the file does not exist."""
        if not self._state_path.exists():
            logger.info("No existing state file at %s", self._state_path)
            return None
        try:
            self._state = json.loads(self._state_path.read_text(encoding="utf-8"))
            logger.info("Loaded state from %s", self._state_path)
            return self._state
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load state from %s: %s", self._state_path, exc)
            return None

    def save_state(self, state: dict[str, Any]) -> None:
        """Persist *state* to ``state.json``, creating parent dirs as needed."""
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_path.write_text(
            json.dumps(state, indent=2, default=str, ensure_ascii=False),
            encoding="utf-8",
        )
        self._state = state
        logger.debug("State saved to %s", self._state_path)

    def init_state(
        self,
        job_id: str,
        mode: str,
        filters: list[str],
    ) -> dict[str, Any]:
        """Create and persist a fresh ``state.json`` for a new run."""
        state: dict[str, Any] = {
            "version": _STATE_VERSION,
            "job_id": job_id,
            "mode": mode,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "filters": filters,
            "counts": {
                "total_listed": 0,
                "total_filtered": 0,
                "processed_success": 0,
                "processed_failed": 0,
                "skipped": 0,
                "cleanup_performed": 0,
                "quarantined": 0,
            },
            "current_batch": 0,
            "total_batches": 0,
            "last_processed_item": None,
            "failure_stats": {
                "consecutive_failures": 0,
                "current_batch_failures": 0,
                "total_metadata_failures": 0,
                "total_delete_failures": 0,
            },
            "last_error": None,
            "avg_processing_time_s": 0.0,
        }
        self.save_state(state)
        return state

    def update_progress(self, item: DriveItem, result: ProcessingResult) -> None:
        """Increment counters in ``state.json`` after one item is processed."""
        if self._state is None:
            logger.warning("update_progress called before state was initialised")
            return

        counts = self._state["counts"]
        failure_stats = self._state["failure_stats"]

        if result.status == ProcessingStatus.SUCCESS:
            counts["processed_success"] += 1
            failure_stats["consecutive_failures"] = 0
        elif result.status == ProcessingStatus.FAILED:
            counts["processed_failed"] += 1
            failure_stats["consecutive_failures"] += 1
            failure_stats["current_batch_failures"] += 1
            self._state["last_error"] = {
                "item_id": result.item_id,
                "item_name": result.item_name,
                "error": result.error_message or "unknown",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        elif result.status == ProcessingStatus.SKIPPED:
            counts["skipped"] += 1
        elif result.status == ProcessingStatus.CLEANUP:
            counts["cleanup_performed"] += 1

        self._state["last_processed_item"] = {
            "id": item.id,
            "name": item.name,
        }

        # Running average of processing time
        total_done = counts["processed_success"] + counts["processed_failed"]
        if total_done > 0 and result.processing_time_s is not None:
            prev_avg = self._state["avg_processing_time_s"]
            self._state["avg_processing_time_s"] = (
                prev_avg * (total_done - 1) + result.processing_time_s
            ) / total_done

        self.save_state(self._state)

    def set_batch_info(self, current_batch: int, total_batches: int) -> None:
        """Update current/total batch counters in state."""
        if self._state is None:
            return
        self._state["current_batch"] = current_batch
        self._state["total_batches"] = total_batches
        self._state["failure_stats"]["current_batch_failures"] = 0
        self.save_state(self._state)

    def finalise(self, exit_code: int, exit_reason: str | None = None) -> None:
        """Mark the run as finished in ``state.json``."""
        if self._state is None:
            return
        self._state["status"] = "finished"
        self._state["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._state["exit_code"] = exit_code
        self._state["exit_reason"] = exit_reason
        self.save_state(self._state)

    # ── delta_token.json ─────────────────────────────────────────────

    def load_delta_token(self) -> str | None:
        """Load ``delta_token.json`` and return the stored delta link, or ``None``."""
        if not self._delta_token_path.exists():
            logger.info("No delta token file at %s", self._delta_token_path)
            return None
        try:
            data = json.loads(self._delta_token_path.read_text(encoding="utf-8"))
            link = data.get("delta_link")
            if link:
                logger.info(
                    "Loaded delta token obtained at %s (%d items)",
                    data.get("obtained_at", "?"),
                    data.get("items_returned", 0),
                )
            return link
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load delta token: %s", exc)
            return None

    def save_delta_token(
        self, delta_link: str, mode: str, items_count: int
    ) -> None:
        """Persist a new delta token to ``delta_token.json``."""
        data = {
            "version": _DELTA_TOKEN_VERSION,
            "delta_link": delta_link,
            "obtained_at": datetime.now(timezone.utc).isoformat(),
            "mode_when_obtained": mode,
            "items_returned": items_count,
        }
        self._delta_token_path.parent.mkdir(parents=True, exist_ok=True)
        self._delta_token_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Delta token saved to %s", self._delta_token_path)
