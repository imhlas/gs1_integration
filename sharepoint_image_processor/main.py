"""CLI entry point — spec v5 section 4.

This module owns:
- CLI argument parsing (typer)
- Databricks secret injection (bootstrap)
- Config validation (fail-fast)
- Launching ``Orchestrator.run()``

It does NOT contain business logic.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Optional

import structlog
import typer

app = typer.Typer(
    name="sharepoint-image-processor",
    help="SharePoint product image background removal pipeline.",
    add_completion=False,
)


# ── Databricks secret injection ─────────────────────────────────────

def inject_databricks_secrets() -> None:
    """Inject Databricks Secrets as environment variables.

    Called ONLY in a Databricks environment, before ``AppConfig`` is
    created.  This is the sole place outside ``config.py`` that touches
    ``os.environ``, as mandated by the spec.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

        secret_map = {
            # tenant_id has a default in AppConfig — no secret needed
            "AZURE_CLIENT_ID": ("gs1-kv", "sharepoint-client-id"),
            "AZURE_CLIENT_SECRET": ("gs1-kv", "sharepoint-client-secret"),
        }
        for env_var, (scope, key) in secret_map.items():
            if env_var not in os.environ:
                os.environ[env_var] = dbutils.secrets.get(scope=scope, key=key)
    except ImportError:
        pass  # Not a Databricks environment — use .env or env vars


# ── Logging setup ────────────────────────────────────────────────────

def _configure_logging(level: str) -> None:
    """Configure structlog + stdlib logging."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=log_level,
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
            if log_level <= logging.DEBUG
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


# ── CLI ──────────────────────────────────────────────────────────────

@app.command()
def main(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        help="Run mode: full | incremental | sample",
    ),
    sample_size: Optional[int] = typer.Option(
        None,
        "--sample-size",
        help="Max images to process (only with --mode sample).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="List and report without making changes.",
    ),
    filter: Optional[list[str]] = typer.Option(
        None,
        "--filter",
        help="Metadata filter KEY=VALUE (repeatable).",
    ),
    env: str = typer.Option(
        "development",
        "--env",
        help="Environment: development | production.",
    ),
    review_only: bool = typer.Option(
        False,
        "--review-only",
        help="Generate review gallery from existing samples (no processing).",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Enable DEBUG-level logging.",
    ),
) -> None:
    """SharePoint Image Processor — product image background removal pipeline."""

    # ── Validate forbidden combinations ──────────────────────────────
    if mode not in ("full", "incremental", "sample"):
        typer.echo(f"ERROR: Invalid mode '{mode}'. Must be full, incremental, or sample.", err=True)
        raise typer.Exit(code=2)

    if sample_size is not None and mode != "sample":
        typer.echo("ERROR: --sample-size can only be used with --mode sample.", err=True)
        raise typer.Exit(code=2)

    if dry_run and review_only:
        typer.echo("ERROR: --dry-run and --review-only cannot be combined.", err=True)
        raise typer.Exit(code=2)

    # Default sample size for sample mode
    if mode == "sample" and sample_size is None:
        sample_size = 100

    # ── Logging ──────────────────────────────────────────────────────
    log_level = "DEBUG" if verbose else "INFO"
    _configure_logging(log_level)

    # ── Databricks secret injection ──────────────────────────────────
    inject_databricks_secrets()

    # ── Config validation (fail-fast) ────────────────────────────────
    from pydantic import ValidationError

    from sharepoint_image_processor.core.config import AppConfig

    try:
        config = AppConfig()
    except ValidationError as exc:
        print(f"FATAL: Configuration error — missing secrets:\n{exc}", file=sys.stderr)
        sys.exit(2)

    # Override log level from CLI if verbose
    if verbose:
        config.log_level = "DEBUG"

    # ── Run pipeline ─────────────────────────────────────────────────
    from sharepoint_image_processor.agents.orchestrator import Orchestrator

    orchestrator = Orchestrator(config)
    filters = list(filter) if filter else []

    exit_code = asyncio.run(
        orchestrator.run(
            mode=mode,
            dry_run=dry_run,
            filters=filters,
            sample_size=sample_size,
            review_only=review_only,
        )
    )

    raise typer.Exit(code=exit_code)


if __name__ == "__main__":
    app()
