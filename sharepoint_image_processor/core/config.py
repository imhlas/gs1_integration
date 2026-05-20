"""Application configuration — spec v5 section 2 + Secret Management Strategy.

All runtime code reads configuration **only** through ``AppConfig``.
No other module may call ``os.environ`` / ``os.getenv`` directly
(except ``main.py`` bootstrap for Databricks secret injection).
"""

from pydantic_settings import BaseSettings


class AppConfig(BaseSettings):
    """Centralised, validated configuration.

    Values are resolved in this order (pydantic-settings default):
    1. Environment variables  (set by CI, Databricks injection, or shell)
    2. ``.env`` file          (local development)
    3. Defaults defined here
    """

    # ── Azure / SharePoint ───────────────────────────────────────────
    azure_tenant_id: str = "003bf88f-5447-4afe-907b-8c4ca7f0d200"  # Lejos Azure AD tenant (public, not a secret)
    azure_client_id: str
    azure_client_secret: str
    sharepoint_site_url: str = "lejosfi.sharepoint.com:/sites/Insights"
    sharepoint_library_name: str = "GS1 Tuotekuvat"

    # ── Performance ──────────────────────────────────────────────────
    rembg_concurrency: int = 3
    graph_api_concurrency: int = 10
    batch_size: int = 100
    rembg_model: str = "u2net"

    # ── Image limits ─────────────────────────────────────────────────
    max_image_size_mb: int = 100
    min_image_size_px: int = 50
    crop_padding_px: int = 10
    min_opaque_ratio: float = 0.05

    # ── Disk ─────────────────────────────────────────────────────────
    min_disk_free_gb: int = 1
    temp_dir: str = "/tmp/image-processor"

    # ── Quality assurance ────────────────────────────────────────────
    review_interval: int = 500
    review_output_dir: str = "/mnt/review"
    sample_accept_threshold: float = 0.90

    # ── Fail policy ──────────────────────────────────────────────────
    max_consecutive_failures: int = 10
    max_failure_rate_percent: int = 15
    max_low_confidence_rate: int = 20
    max_metadata_failures: int = 5

    # ── State & logging ──────────────────────────────────────────────
    state_path: str = "dbfs:/mnt/image-processor/state/state.json"
    delta_token_path: str = "dbfs:/mnt/image-processor/state/delta_token.json"
    log_level: str = "INFO"
    log_file: str = "dbfs:/mnt/image-processor/logs/processing.log"
    report_output: str = "dbfs:/mnt/image-processor/reports/report.json"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }
