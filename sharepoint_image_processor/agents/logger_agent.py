"""Structured logging, fail-policy checking, and report generation.

Spec v5 section 5 — ``LoggerAgent`` owns:
- Structlog JSON logging for results and quarantine
- Fail-policy evaluation (consecutive failures, rate, metadata, low confidence)
- Writing ``report.json`` and ``quarantine_report.json``

It does NOT do image processing or uploads.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import structlog

from sharepoint_image_processor.core.config import AppConfig
from sharepoint_image_processor.core.models import (
    JobStats,
    ProcessingResult,
    ProcessingStatus,
    QuarantineRecord,
)

logger = logging.getLogger(__name__)
slog = structlog.get_logger()

_REPORT_VERSION = 1


class LoggerAgent:
    """Logging, fail-policy checking, and report writing."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._quarantine_records: list[QuarantineRecord] = []
        self._failed_items: list[dict] = []
        self._format_counts: dict[str, int] = {}
        self._consecutive_failures: int = 0

    @property
    def quarantine_records(self) -> list[QuarantineRecord]:
        return list(self._quarantine_records)

    # ── Logging ──────────────────────────────────────────────────────

    def log_result(self, result: ProcessingResult) -> None:
        """Emit a structured log entry for a single processing result."""
        slog.info(
            "item_processed",
            item_id=result.item_id,
            item_name=result.item_name,
            status=result.status.value,
            classification=result.classification.value,
            confidence=result.confidence_score,
            error=result.error_message,
            time_s=result.processing_time_s,
        )
        if result.status == ProcessingStatus.FAILED:
            self._consecutive_failures += 1
            self._failed_items.append({
                "id": result.item_id,
                "name": result.item_name,
                "error": result.error_message or "unknown",
            })
        elif result.status == ProcessingStatus.SUCCESS:
            self._consecutive_failures = 0
        if result.image_format:
            key = result.image_format.value
            self._format_counts[key] = self._format_counts.get(key, 0) + 1

    def log_quarantine(self, record: QuarantineRecord) -> None:
        """Log and accumulate a quarantine record."""
        slog.warning(
            "item_quarantined",
            item_id=record.item_id,
            item_name=record.item_name,
            reason=record.reason,
        )
        self._quarantine_records.append(record)

    # ── Fail policy ──────────────────────────────────────────────────

    def check_fail_policy(
        self, stats: JobStats, config: AppConfig
    ) -> tuple[bool, str | None]:
        """Evaluate fail-policy limits. Returns ``(should_continue, reason)``.

        Checks (any triggers stop):
        1. Consecutive failures > max_consecutive_failures
        2. Overall failure rate > max_failure_rate_percent
        3. Low-confidence rate > max_low_confidence_rate
        4. Total metadata failures > max_metadata_failures
        """
        total_done = stats.processed_success + stats.processed_failed
        if total_done == 0:
            return True, None

        # 1. Consecutive failures
        if self._consecutive_failures >= config.max_consecutive_failures:
            reason = (
                f"Consecutive failures ({self._consecutive_failures}) "
                f"reached limit {config.max_consecutive_failures}"
            )
            logger.error(reason)
            return False, reason

        # 2. Failure rate
        fail_rate = (stats.processed_failed / total_done) * 100
        if fail_rate > config.max_failure_rate_percent:
            reason = f"Failure rate {fail_rate:.1f}% exceeds limit {config.max_failure_rate_percent}%"
            logger.error(reason)
            return False, reason

        # 3. Low confidence rate
        if total_done > 0:
            lc_rate = (stats.low_confidence_count / total_done) * 100
            if lc_rate > config.max_low_confidence_rate:
                reason = f"Low-confidence rate {lc_rate:.1f}% exceeds limit {config.max_low_confidence_rate}%"
                logger.error(reason)
                return False, reason

        # 4. Metadata failures (subset of total failures — tracked via
        #    error messages containing "missing_required" or "metadata").
        metadata_fail_count = sum(
            1 for f in self._failed_items
            if "required" in (f.get("error") or "").lower()
            or "metadata" in (f.get("error") or "").lower()
        )
        if metadata_fail_count > config.max_metadata_failures:
            reason = f"Metadata failures ({metadata_fail_count}) exceed limit {config.max_metadata_failures}"
            logger.error(reason)
            return False, reason

        return True, None

    # ── Reports ──────────────────────────────────────────────────────

    def write_report(self, stats: JobStats, output_path: str) -> None:
        """Write ``report.json`` matching the spec section 3 schema."""
        path = _resolve_path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        duration_s = 0
        if stats.finished_at and stats.started_at:
            duration_s = int((stats.finished_at - stats.started_at).total_seconds())

        report = {
            "version": _REPORT_VERSION,
            "job_id": stats.job_id,
            "mode": stats.mode,
            "started_at": stats.started_at.isoformat(),
            "finished_at": stats.finished_at.isoformat() if stats.finished_at else None,
            "duration_s": duration_s,
            "counts": {
                "total_listed": stats.total_listed,
                "total_filtered": stats.total_filtered,
                "processed_success": stats.processed_success,
                "processed_failed": stats.processed_failed,
                "skipped": stats.skipped,
                "cleanup_performed": stats.cleanup_performed,
                "quarantined": stats.quarantined,
                "low_confidence": stats.low_confidence_count,
            },
            "avg_processing_time_s": stats.avg_processing_time_s,
            "format_breakdown": dict(self._format_counts),
            "exit_code": stats.exit_code,
            "exit_reason": stats.exit_reason,
            "filters": stats.filters_applied,
            "failed_items": self._failed_items,
        }
        path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        logger.info("Report written to %s", path)

    def write_quarantine_report(
        self, records: list[QuarantineRecord], output_path: str
    ) -> None:
        """Write ``quarantine_report.json`` matching spec section 3 schema."""
        # Derive quarantine path from the report output path
        base_path = _resolve_path(output_path)
        quarantine_path = base_path.parent / "quarantine_report.json"
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)

        report = {
            "version": _REPORT_VERSION,
            "job_id": "",  # Will be set from context
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "records": [r.model_dump(mode="json") for r in records],
        }
        quarantine_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        logger.info("Quarantine report written to %s (%d records)", quarantine_path, len(records))


def _resolve_path(raw: str) -> Path:
    """Translate ``dbfs:/…`` to FUSE mount ``/dbfs/…``."""
    if raw.startswith("dbfs:"):
        return Path("/dbfs" + raw[5:])
    return Path(raw)
