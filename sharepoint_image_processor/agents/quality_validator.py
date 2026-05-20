"""Technical quality validation and review sampling — spec v5 section 5.

``QualityValidator`` owns:
- Technical validation (alpha, opaque ratio, dimensions, edges)
- Red-flag logic for review
- Review sample creation (side-by-side original + processed)

It does NOT do image processing or uploads.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import numpy as np
from PIL import Image

from sharepoint_image_processor.core.config import AppConfig
from sharepoint_image_processor.core.models import ReviewSample

logger = logging.getLogger(__name__)


class QualityValidator:
    """Validates processed images and generates review samples."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._review_samples: list[ReviewSample] = []

    @property
    def review_samples(self) -> list[ReviewSample]:
        return list(self._review_samples)

    # ── Technical validation ─────────────────────────────────────────

    def validate_technical(self, image: Image.Image) -> tuple[bool, str]:
        """Validate the processed image against technical criteria.

        Checks:
        1. Has alpha channel (RGBA)
        2. Opaque pixel ratio ≥ ``min_opaque_ratio``
        3. Dimensions ≥ ``min_image_size_px`` on both axes
        4. Not fully transparent

        Returns ``(passed, reason)`` — *reason* is ``""`` on success.
        """
        # Must be RGBA
        if image.mode != "RGBA":
            return False, "no_alpha_channel"

        w, h = image.size
        if w < self._config.min_image_size_px or h < self._config.min_image_size_px:
            return False, f"too_small:{w}x{h}"

        alpha = np.array(image.getchannel("A"))
        total_pixels = alpha.size
        if total_pixels == 0:
            return False, "zero_pixels"

        opaque_count = int(np.count_nonzero(alpha > 128))
        opaque_ratio = opaque_count / total_pixels

        if opaque_ratio < self._config.min_opaque_ratio:
            return False, f"low_opaque_ratio:{opaque_ratio:.3f}"

        # Bounding box check — fully transparent
        if image.getbbox() is None:
            return False, "fully_transparent"

        return True, ""

    # ── Review flagging ──────────────────────────────────────────────

    def should_flag_for_review(
        self, confidence: float, pixel_ratio: float
    ) -> tuple[bool, str]:
        """Determine whether the image should be flagged for manual review.

        Flags are raised when:
        - Confidence < sample_accept_threshold
        - Pixel ratio is outside the normal band (< 0.05 or > 0.90)
        """
        reasons: list[str] = []

        if confidence < self._config.sample_accept_threshold:
            reasons.append(f"low_confidence:{confidence:.3f}")

        if pixel_ratio < self._config.min_opaque_ratio:
            reasons.append(f"low_pixel_ratio:{pixel_ratio:.3f}")
        elif pixel_ratio > 0.90:
            reasons.append(f"high_pixel_ratio:{pixel_ratio:.3f}")

        flagged = len(reasons) > 0
        return flagged, "; ".join(reasons)

    # ── Review sample creation ───────────────────────────────────────

    def create_review_sample(
        self,
        item_id: str,
        item_name: str,
        original_path: Path,
        processed_image: Image.Image,
        confidence: float,
    ) -> ReviewSample:
        """Save a side-by-side review sample and return the metadata record.

        The original and processed images are copied into the review
        output directory for later gallery generation.
        """
        review_dir = Path(self._config.review_output_dir)
        review_dir.mkdir(parents=True, exist_ok=True)

        stem = original_path.stem
        orig_dest = review_dir / f"{stem}_original{original_path.suffix}"
        proc_dest = review_dir / f"{stem}_processed.png"

        shutil.copy2(original_path, orig_dest)
        processed_image.save(proc_dest, "PNG")

        # Compute pixel ratio
        alpha = np.array(processed_image.getchannel("A"))
        total = alpha.size
        opaque = int(np.count_nonzero(alpha > 128)) if total > 0 else 0
        pixel_ratio = opaque / total if total > 0 else 0.0

        flagged, flag_reason = self.should_flag_for_review(confidence, pixel_ratio)

        sample = ReviewSample(
            item_id=item_id,
            item_name=item_name,
            original_path=str(orig_dest),
            processed_path=str(proc_dest),
            confidence_score=confidence,
            pixel_ratio=pixel_ratio,
            flagged=flagged,
            flag_reason=flag_reason if flagged else None,
        )
        self._review_samples.append(sample)
        logger.info(
            "Review sample created for %s (confidence=%.3f, flagged=%s)",
            item_name, confidence, flagged,
        )
        return sample
