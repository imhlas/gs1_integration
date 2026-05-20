"""Pipeline orchestrator — spec v5 E2E workflow (Source of Truth).

``Orchestrator`` owns:
- Coordination of all agents and services
- Run modes (full / incremental / sample)
- Batch splitting and processing loop
- SIGINT / SIGTERM graceful shutdown
- Dry-run report
- Review gallery generation trigger

It does NOT process images or make HTTP calls directly.
Data flows downward: orchestrator → agents → services.
"""

from __future__ import annotations

import asyncio
import io
import logging
import shutil
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog

from sharepoint_image_processor.agents.format_router import FormatRouter
from sharepoint_image_processor.agents.logger_agent import LoggerAgent
from sharepoint_image_processor.agents.quality_validator import QualityValidator
from sharepoint_image_processor.agents.sharepoint_agent import SharePointAgent
from sharepoint_image_processor.agents.upload_agent import UploadAgent
from sharepoint_image_processor.core.config import AppConfig
from sharepoint_image_processor.core.models import (
    DriveItem,
    ImageFormat,
    ItemClassification,
    JobStats,
    ProcessingResult,
    ProcessingStatus,
    QuarantineRecord,
    ReviewSample,
)
from sharepoint_image_processor.core.state_manager import StateManager
from sharepoint_image_processor.services.auth import AuthService
from sharepoint_image_processor.services.graph_client import GraphClient
from sharepoint_image_processor.services.image_service import ImageService

logger = logging.getLogger(__name__)
slog = structlog.get_logger()

EXIT_OK = 0
EXIT_FAIL_POLICY = 1
EXIT_FATAL = 2
EXIT_INTERRUPTED = 3

T = __import__("typing").TypeVar("T")


def _chunk(items: list, batch_size: int) -> list[list]:
    """Split *items* into sublists of at most *batch_size* elements."""
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _check_disk_space(min_free_gb: int, path: str = "/") -> bool:
    """Return True when at least *min_free_gb* GB is free on *path*."""
    try:
        usage = shutil.disk_usage(Path(path))
        free_gb = usage.free / (1024 ** 3)
        return free_gb >= min_free_gb
    except OSError:
        return True


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _image_to_png_bytes(image: "Image.Image") -> bytes:  # noqa: F821
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


class Orchestrator:
    """Top-level coordinator for the image processing pipeline."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._shutdown_requested = False

    # ── Signal handling ──────────────────────────────────────────────

    def _install_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM for graceful shutdown."""
        def _handler(sig: int, _frame: object) -> None:
            logger.warning("Received signal %d — requesting graceful shutdown", sig)
            self._shutdown_requested = True

        try:
            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)
        except (OSError, ValueError):
            # signal handlers can only be set from the main thread
            pass

    # ── Main entry point ─────────────────────────────────────────────

    async def run(
        self,
        mode: str,
        dry_run: bool,
        filters: list[str],
        sample_size: int | None,
        review_only: bool = False,
    ) -> int:
        """Execute the full pipeline. Returns an exit code (0/1/2/3)."""
        self._install_signal_handlers()
        config = self._config

        # ── VAIHE 1: ALUSTUS ────────────────────────────────────────
        state_mgr = StateManager(config.state_path, config.delta_token_path)

        # ── VAIHE 2: AUTENTIKOINTI ──────────────────────────────────
        auth = AuthService(config)
        try:
            await auth.get_access_token()
        except RuntimeError as exc:
            logger.error("Authentication failed: %s", exc)
            return EXIT_FATAL
        graph = GraphClient(auth, config)

        try:
            return await self._run_inner(
                mode, dry_run, filters, sample_size, review_only,
                config, state_mgr, graph,
            )
        finally:
            await graph.close()

    async def _run_inner(
        self,
        mode: str,
        dry_run: bool,
        filters: list[str],
        sample_size: int | None,
        review_only: bool,
        config: AppConfig,
        state_mgr: StateManager,
        graph: GraphClient,
    ) -> int:
        # ── Review-only mode ────────────────────────────────────────
        if review_only:
            self._generate_review_html([], config.review_output_dir)
            return EXIT_OK

        # ── VAIHE 3: SHAREPOINT RESOLUTION ──────────────────────────
        try:
            site_id = await graph.resolve_site_id(config.sharepoint_site_url)
            drive_id = await graph.resolve_drive_id(
                site_id, config.sharepoint_library_name
            )
        except PermissionError as exc:
            logger.error("Permission denied: %s", exc)
            return EXIT_FATAL
        except Exception as exc:
            logger.error("SharePoint resolution failed: %s", exc)
            return EXIT_FATAL

        # ── VAIHE 4: DELTA TOKEN ────────────────────────────────────
        delta_token = None
        if mode == "incremental":
            delta_token = state_mgr.load_delta_token()
            if delta_token is None:
                logger.warning("No delta token — falling back to full listing")

        # ── VAIHE 5: LISTAUS ────────────────────────────────────────
        sp_agent = SharePointAgent(graph, config)
        classified_items, new_delta_link = await sp_agent.list_and_classify(
            drive_id, delta_token, filters
        )

        # ── VAIHE 6: DRY-RUN ───────────────────────────────────────
        if dry_run:
            self._print_dry_run_report(classified_items, filters)
            return EXIT_OK

        # ── VAIHE 7: SAMPLE RAJAUS ──────────────────────────────────
        processable = [
            (item, cls)
            for item, cls in classified_items
            if cls == ItemClassification.PROCESS
        ]
        cleanups = [
            (item, cls)
            for item, cls in classified_items
            if cls.value.startswith("cleanup")
        ]
        quarantines = [
            (item, cls)
            for item, cls in classified_items
            if cls == ItemClassification.QUARANTINE_DUPLICATE
        ]

        if mode == "sample" and sample_size:
            processable = processable[:sample_size]

        # ── VAIHE 8: INIT SERVICES ──────────────────────────────────
        image_svc = ImageService(config)
        format_router = FormatRouter(image_svc)
        validator = QualityValidator(config)
        upload_agent = UploadAgent(graph, config)
        logger_agent = LoggerAgent(config)

        stats = JobStats(
            job_id=_now_iso(),
            mode=mode,
            started_at=_now(),
        )
        stats.total_listed = len(classified_items)
        stats.total_filtered = len(processable)
        stats.filters_applied = list(filters)

        state_mgr.init_state(stats.job_id, mode, filters)

        # Register quarantines
        for item, _cls in quarantines:
            record = QuarantineRecord(
                item_id=item.id,
                item_name=item.name,
                reason="manual_duplicate",
                parent_path=item.parent_path,
            )
            logger_agent.log_quarantine(record)
            stats.quarantined += 1

        # ── VAIHE 9: BATCH-KÄSITTELY ───────────────────────────────
        batches = _chunk(
            [item for item, _cls in processable],
            config.batch_size,
        )
        rembg_sem = asyncio.Semaphore(config.rembg_concurrency)
        graph_sem = asyncio.Semaphore(config.graph_api_concurrency)

        for batch_idx, batch in enumerate(batches):
            if self._shutdown_requested:
                stats.exit_code = EXIT_INTERRUPTED
                stats.exit_reason = "SIGINT/SIGTERM"
                break

            state_mgr.set_batch_info(batch_idx + 1, len(batches))

            # 9a. Disk space check
            if not _check_disk_space(config.min_disk_free_gb, config.temp_dir):
                logger.error("Disk space below %d GB — stopping", config.min_disk_free_gb)
                stats.exit_code = EXIT_FATAL
                stats.exit_reason = "disk_space_low"
                break

            # 9b. Download batch
            processable_for_dl = [(item, ItemClassification.PROCESS) for item in batch]
            downloaded = await sp_agent.download_batch(
                drive_id, processable_for_dl, config.temp_dir, graph_sem
            )

            # 9c. Process concurrently
            async def _process_one(
                item: DriveItem, local_path: Path
            ) -> ProcessingResult:
                if self._shutdown_requested:
                    return ProcessingResult(
                        item_id=item.id,
                        item_name=item.name,
                        status=ProcessingStatus.SKIPPED,
                        classification=ItemClassification.PROCESS,
                        error_message="shutdown_requested",
                    )
                return await self._process_single_image(
                    item, local_path, format_router, validator,
                    upload_agent, drive_id, rembg_sem, stats,
                )

            batch_results = await asyncio.gather(
                *[_process_one(item, path) for item, path in downloaded],
                return_exceptions=True,
            )

            # Handle exceptions from gather
            for i, res in enumerate(batch_results):
                if isinstance(res, Exception):
                    item, _path = downloaded[i]
                    result = ProcessingResult(
                        item_id=item.id,
                        item_name=item.name,
                        status=ProcessingStatus.FAILED,
                        classification=ItemClassification.PROCESS,
                        error_message=str(res),
                    )
                    batch_results[i] = result

            # Log and update state for each result
            for i, res in enumerate(batch_results):
                if not isinstance(res, ProcessingResult):
                    continue
                logger_agent.log_result(res)
                item, _path = downloaded[i] if i < len(downloaded) else (None, None)
                if item:
                    state_mgr.update_progress(item, res)
                # Update stats
                if res.status == ProcessingStatus.SUCCESS:
                    stats.processed_success += 1
                elif res.status == ProcessingStatus.FAILED:
                    stats.processed_failed += 1
                elif res.status == ProcessingStatus.SKIPPED:
                    stats.skipped += 1
                if (
                    res.confidence_score is not None
                    and res.confidence_score < self._config.sample_accept_threshold
                ):
                    stats.low_confidence_count += 1

            # 9d. Fail policy
            should_continue, reason = logger_agent.check_fail_policy(stats, self._config)
            if not should_continue:
                stats.exit_code = EXIT_FAIL_POLICY
                stats.exit_reason = reason
                break

            # 9e. Cleanup temp
            self._cleanup_temp(config.temp_dir)
            slog.info(
                "batch_complete",
                batch=batch_idx + 1,
                total=len(batches),
                success=stats.processed_success,
                failed=stats.processed_failed,
            )

        # ── VAIHE 10: CLEANUP ───────────────────────────────────────
        for item, cls in cleanups:
            if cls == ItemClassification.CLEANUP_DELETE_OLD:
                await upload_agent.delete_old_item(drive_id, item.id)
                stats.cleanup_performed += 1
            elif cls == ItemClassification.CLEANUP_DELETE_ORPHAN:
                await upload_agent.cleanup_orphan(drive_id, item.id)
                stats.cleanup_performed += 1

        # ── VAIHE 11: SAVE DELTA TOKEN ──────────────────────────────
        if new_delta_link:
            state_mgr.save_delta_token(new_delta_link, mode, stats.total_listed)
        elif filters:
            logger.info(
                "Filtered run — delta token NOT updated. "
                "Next incremental run will use the previous token."
            )

        # ── VAIHE 12: REPORTS ───────────────────────────────────────
        stats.finished_at = _now()
        # Compute average processing time
        total_done = stats.processed_success + stats.processed_failed
        if total_done > 0:
            stats.avg_processing_time_s = round(stats.avg_processing_time_s, 2)

        logger_agent.write_report(stats, config.report_output)
        logger_agent.write_quarantine_report(
            logger_agent.quarantine_records, config.report_output
        )
        state_mgr.finalise(stats.exit_code, stats.exit_reason)

        # ── VAIHE 13: REVIEW GALLERY ────────────────────────────────
        if validator.review_samples:
            self._generate_review_html(
                validator.review_samples, config.review_output_dir
            )

        return stats.exit_code

    # ── Single image processing ──────────────────────────────────────

    async def _process_single_image(
        self,
        item: DriveItem,
        local_path: Path,
        format_router: FormatRouter,
        validator: QualityValidator,
        upload_agent: UploadAgent,
        drive_id: str,
        rembg_sem: asyncio.Semaphore,
        stats: JobStats,
    ) -> ProcessingResult:
        """Process one image: detect → rembg → crop → validate → upload."""
        t0 = time.monotonic()
        try:
            # 1. Detect format
            fmt = format_router.detect(local_path)
            if fmt == ImageFormat.UNKNOWN:
                return ProcessingResult(
                    item_id=item.id,
                    item_name=item.name,
                    status=ProcessingStatus.SKIPPED,
                    classification=ItemClassification.PROCESS,
                    error_message="unknown_format",
                    processing_time_s=time.monotonic() - t0,
                )

            # 2. Process (rembg + crop) under semaphore
            async with rembg_sem:
                processed_img, fmt, confidence = await asyncio.to_thread(
                    format_router.process, local_path
                )

            # 3. Technical validation
            valid, reason = validator.validate_technical(processed_img)
            if not valid:
                return ProcessingResult(
                    item_id=item.id,
                    item_name=item.name,
                    status=ProcessingStatus.FAILED,
                    classification=ItemClassification.PROCESS,
                    image_format=fmt,
                    confidence_score=confidence,
                    error_message=f"validation:{reason}",
                    processing_time_s=time.monotonic() - t0,
                )

            # 4. Review sampling (every N images)
            if (
                stats.processed_success > 0
                and stats.processed_success % self._config.review_interval == 0
            ):
                validator.create_review_sample(
                    item.id, item.name, local_path, processed_img, confidence
                )

            # 5. Upload transaction
            png_bytes = _image_to_png_bytes(processed_img)
            result = await upload_agent.execute_transaction(
                drive_id, item, png_bytes
            )
            result.confidence_score = confidence
            result.image_format = fmt
            result.processing_time_s = time.monotonic() - t0
            return result

        except Exception as exc:
            return ProcessingResult(
                item_id=item.id,
                item_name=item.name,
                status=ProcessingStatus.FAILED,
                classification=ItemClassification.PROCESS,
                error_message=str(exc),
                processing_time_s=time.monotonic() - t0,
            )

    # ── Dry-run report ───────────────────────────────────────────────

    @staticmethod
    def _print_dry_run_report(
        classified: list[tuple[DriveItem, ItemClassification]],
        filters: list[str],
    ) -> None:
        counts: dict[str, int] = {}
        for _, cls in classified:
            counts[cls.value] = counts.get(cls.value, 0) + 1

        print("\n" + "=" * 60)
        print("DRY-RUN REPORT")
        print("=" * 60)
        if filters:
            print(f"Filters: {filters}")
        print(f"Total items: {len(classified)}")
        print("-" * 40)
        for cls_name, count in sorted(counts.items()):
            print(f"  {cls_name:30s} {count:>6d}")
        print("=" * 60 + "\n")

    # ── Temp cleanup ─────────────────────────────────────────────────

    @staticmethod
    def _cleanup_temp(temp_dir: str) -> None:
        path = Path(temp_dir)
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            path.mkdir(parents=True, exist_ok=True)

    # ── Review gallery ───────────────────────────────────────────────

    @staticmethod
    def _generate_review_html(
        samples: list[ReviewSample], output_dir: str
    ) -> None:
        """Generate a simple HTML gallery for review samples."""
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        html_path = out / "review.html"

        rows: list[str] = []
        for s in samples:
            flag_class = ' class="flagged"' if s.flagged else ""
            rows.append(f"""
        <tr{flag_class}>
            <td>{s.item_name}</td>
            <td><img src="{Path(s.original_path).name}" width="200"></td>
            <td><img src="{Path(s.processed_path).name}" width="200"></td>
            <td>{s.confidence_score:.3f}</td>
            <td>{s.pixel_ratio:.3f}</td>
            <td>{"YES: " + (s.flag_reason or "") if s.flagged else "no"}</td>
        </tr>""")

        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Image Processor Review Gallery</title>
    <style>
        body {{ font-family: sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ccc; padding: 8px; text-align: center; }}
        th {{ background: #f5f5f5; }}
        .flagged {{ background: #ffe0e0; }}
        img {{ max-height: 200px; }}
    </style>
</head>
<body>
    <h1>Review Gallery — {len(samples)} samples</h1>
    <table>
    <tr>
        <th>Item</th><th>Original</th><th>Processed</th>
        <th>Confidence</th><th>Pixel ratio</th><th>Flagged</th>
    </tr>
    {"".join(rows)}
    </table>
</body>
</html>"""
        html_path.write_text(html, encoding="utf-8")
        logger.info("Review gallery written to %s", html_path)
