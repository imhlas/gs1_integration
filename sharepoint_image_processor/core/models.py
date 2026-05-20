"""Pydantic data models — spec v5 section 2.

These models are contracts. Field names, types, and defaults match the spec exactly.
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ItemClassification(str, Enum):
    """Classification result for a drive item during the listing phase."""

    PROCESS = "process"
    SKIP_PROCESSED = "skip_processed"
    SKIP_MANUAL_PNG = "skip_manual_png"
    SKIP_UNSUPPORTED = "skip_unsupported"
    SKIP_TOO_SMALL = "skip_too_small"
    SKIP_TOO_LARGE = "skip_too_large"
    SKIP_FOLDER = "skip_folder"
    CLEANUP_DELETE_OLD = "cleanup_delete_old"
    CLEANUP_DELETE_ORPHAN = "cleanup_delete_orphan"
    QUARANTINE_DUPLICATE = "quarantine_duplicate"


class ProcessingStatus(str, Enum):
    """Outcome status for a single image processing operation."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CLEANUP = "cleanup"


class ImageFormat(str, Enum):
    """Detected image format after Pillow inspection."""

    JPEG = "jpeg"
    PNG = "png"
    TIFF_RGB = "tiff_rgb"
    TIFF_CMYK = "tiff_cmyk"
    TIFF_MULTIPAGE = "tiff_multipage"
    UNKNOWN = "unknown"


class DriveItem(BaseModel):
    """Graph API drive item mapped to the internal model.

    Created from a Graph API delta response entry.  The ``stem`` and
    ``extension`` fields are derived automatically from ``name`` via
    ``model_post_init``.
    """

    id: str                   # Graph item ID
    name: str                 # File name  (e.g. tuote_001.jpg)
    size: int                 # Bytes
    mime_type: str            # file.mimeType
    parent_path: str          # parentReference.path  (folder)
    parent_id: str            # parentReference.id
    fields: dict              # listItem.fields  (raw dict)
    # Derived ─────────────────────────────────────────────────────
    stem: str = ""            # Name without extension
    extension: str = ""       # Lower-cased extension incl. dot

    def model_post_init(self, __context: object) -> None:
        if not self.stem and "." in self.name:
            self.stem = self.name.rsplit(".", 1)[0]
            self.extension = "." + self.name.rsplit(".", 1)[1].lower()
        elif not self.stem:
            self.stem = self.name

    @staticmethod
    def from_graph_response(raw: dict) -> "DriveItem":
        """Map a raw Graph API delta item dict into a DriveItem."""
        parent_ref = raw.get("parentReference", {})
        file_info = raw.get("file", {})
        list_item = raw.get("listItem", {})
        return DriveItem(
            id=raw["id"],
            name=raw["name"],
            size=raw.get("size", 0),
            mime_type=file_info.get("mimeType", ""),
            parent_path=parent_ref.get("path", ""),
            parent_id=parent_ref.get("id", ""),
            fields=list_item.get("fields", {}),
        )


class ItemFields(BaseModel):
    """SharePoint library metadata fields (copyable subset)."""

    EAN: str
    BRAND: str
    Tuote: str
    Title: Optional[str] = None                                          # Spec "Otsikko"
    extended_description: Optional[str] = Field(None, alias="_ExtendedDescription")  # Spec "Kuvaus"
    Kesko_x002d_kategoria1: Optional[str] = None
    Kesko_x002d_kategoria2: Optional[str] = None
    Kesko_x002d_kategoria3: Optional[str] = None
    GS1_x002d_kategoria1: Optional[str] = None
    GS1_x002d_kategoria2: Optional[str] = None


class ProcessingResult(BaseModel):
    """Result of processing a single image."""

    item_id: str
    item_name: str
    status: ProcessingStatus
    classification: ItemClassification
    new_item_id: Optional[str] = None
    new_item_name: Optional[str] = None
    image_format: Optional[ImageFormat] = None
    confidence_score: Optional[float] = None   # 0.0 – 1.0
    error_message: Optional[str] = None
    processing_time_s: Optional[float] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class QuarantineRecord(BaseModel):
    """Single row in the quarantine report."""

    item_id: str
    item_name: str
    reason: str                                 # "manual_duplicate", "low_confidence", …
    parent_path: str
    related_item_id: Optional[str] = None
    related_item_name: Optional[str] = None
    confidence_score: Optional[float] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ReviewSample(BaseModel):
    """Image saved into the review gallery."""

    item_id: str
    item_name: str
    original_path: str
    processed_path: str
    confidence_score: float
    pixel_ratio: float                          # opaque pixels / original pixels
    flagged: bool
    flag_reason: Optional[str] = None


class JobStats(BaseModel):
    """Aggregate job statistics written to the final report."""

    job_id: str
    mode: str                                   # full / incremental / sample
    started_at: datetime
    finished_at: Optional[datetime] = None
    total_listed: int = 0
    total_filtered: int = 0
    processed_success: int = 0
    processed_failed: int = 0
    skipped: int = 0
    cleanup_performed: int = 0
    quarantined: int = 0
    low_confidence_count: int = 0
    avg_processing_time_s: float = 0.0
    filters_applied: list[str] = Field(default_factory=list)
    exit_code: int = 0                          # 0=ok, 1=fail policy, 2=fatal
    exit_reason: Optional[str] = None
