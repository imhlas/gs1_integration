"""Image processing service — spec v5 section 5.

``ImageService`` owns rembg, Pillow, crop, CMYK conversion, and
confidence scoring.  It does NOT touch Graph API or download files.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

import numpy as np
from PIL import Image
from rembg import new_session, remove

from sharepoint_image_processor.core.config import AppConfig
from sharepoint_image_processor.core.models import ImageFormat

logger = logging.getLogger(__name__)


class ImageService:
    """Stateful image processing service — initialises the rembg session once."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        logger.info("Initialising rembg session with model '%s'", config.rembg_model)
        self._session = new_session(model_name=config.rembg_model)

    # ── Format detection ─────────────────────────────────────────────

    def detect_format(self, file_path: Path) -> ImageFormat:
        """Detect the image format using Pillow (MIME + mode + n_frames)."""
        try:
            with Image.open(file_path) as img:
                fmt = img.format  # "JPEG", "TIFF", …
                mode = img.mode   # "RGB", "CMYK", "RGBA", …
                n_frames = getattr(img, "n_frames", 1)
        except Exception as exc:
            logger.warning("Cannot open %s: %s", file_path.name, exc)
            return ImageFormat.UNKNOWN

        if fmt == "JPEG":
            return ImageFormat.JPEG
        if fmt == "PNG":
            return ImageFormat.PNG
        if fmt == "TIFF":
            if n_frames > 1:
                return ImageFormat.TIFF_MULTIPAGE
            if mode == "CMYK":
                return ImageFormat.TIFF_CMYK
            return ImageFormat.TIFF_RGB
        return ImageFormat.UNKNOWN

    # ── Background removal ───────────────────────────────────────────

    def remove_background(self, file_path: Path) -> Image.Image:
        """Run rembg on the file and return an RGBA ``Image``."""
        with open(file_path, "rb") as f:
            input_bytes = f.read()
        output_bytes = remove(input_bytes, session=self._session)
        return Image.open(io.BytesIO(output_bytes)).convert("RGBA")

    # ── Crop ─────────────────────────────────────────────────────────

    def crop_to_content(self, image: Image.Image, padding_px: int) -> Image.Image:
        """Crop to the bounding box of non-transparent pixels + padding.

        Padding is clamped so it never exceeds the image boundaries.
        """
        bbox = image.getbbox()
        if bbox is None:
            logger.warning("Image is fully transparent — returning as-is")
            return image

        left, upper, right, lower = bbox
        w, h = image.size
        left = max(0, left - padding_px)
        upper = max(0, upper - padding_px)
        right = min(w, right + padding_px)
        lower = min(h, lower + padding_px)
        return image.crop((left, upper, right, lower))

    # ── CMYK → RGB ───────────────────────────────────────────────────

    @staticmethod
    def convert_cmyk_to_rgb(image: Image.Image) -> Image.Image:
        """Convert a CMYK image to RGB."""
        if image.mode == "CMYK":
            return image.convert("RGB")
        return image

    # ── Confidence scoring ───────────────────────────────────────────

    def calculate_confidence(
        self, original: Image.Image, processed: Image.Image
    ) -> float:
        """Score how cleanly the background was removed (0.0 – 1.0).

        Combines two signals:
        1. **Opaque pixel ratio** — fraction of non-transparent pixels in
           the processed image relative to total pixels in the original.
           Very low ratios signal that too much was removed; very high
           ratios signal that too little was removed.
        2. **Edge smoothness** — a simple measure of how jagged the alpha
           boundary is.  Smoother edges → higher confidence.

        The two signals are combined with equal weight.
        """
        orig_pixels = original.size[0] * original.size[1]
        if orig_pixels == 0:
            return 0.0

        # Opaque ratio
        alpha = np.array(processed.getchannel("A"))
        opaque_count = int(np.count_nonzero(alpha > 128))
        opaque_ratio = opaque_count / orig_pixels

        # Penalise extreme ratios (ideal range roughly 0.05–0.85)
        if opaque_ratio < self._config.min_opaque_ratio:
            ratio_score = opaque_ratio / self._config.min_opaque_ratio * 0.3
        elif opaque_ratio > 0.95:
            ratio_score = 0.5
        else:
            ratio_score = 1.0

        # Edge smoothness: count alpha-boundary transitions
        transitions_h = int(np.sum(np.abs(np.diff((alpha > 128).astype(np.int8), axis=1))))
        transitions_v = int(np.sum(np.abs(np.diff((alpha > 128).astype(np.int8), axis=0))))
        total_transitions = transitions_h + transitions_v
        perimeter_estimate = 2 * (processed.size[0] + processed.size[1])
        if perimeter_estimate > 0:
            edge_ratio = total_transitions / perimeter_estimate
            # Ideal: ~1.0 (clean contour). Noisy: >> 1.0
            edge_score = min(1.0, 1.0 / max(edge_ratio, 0.1))
        else:
            edge_score = 0.5

        confidence = 0.5 * ratio_score + 0.5 * edge_score
        return round(min(1.0, max(0.0, confidence)), 3)

    # ── Full pipeline ────────────────────────────────────────────────

    def process_image(
        self, file_path: Path
    ) -> tuple[Image.Image, ImageFormat, float]:
        """Run the complete processing pipeline on a single file.

        Returns ``(processed_rgba_image, detected_format, confidence)``.
        """
        fmt = self.detect_format(file_path)
        if fmt == ImageFormat.UNKNOWN:
            raise ValueError(f"Unsupported image format: {file_path.name}")

        # Open original for confidence calculation
        original = Image.open(file_path)

        # CMYK → RGB conversion before rembg (rembg expects RGB input)
        if fmt == ImageFormat.TIFF_CMYK:
            rgb_img = self.convert_cmyk_to_rgb(original)
            # Save temp RGB version for rembg
            tmp_path = file_path.with_suffix(".tmp.png")
            rgb_img.save(tmp_path, "PNG")
            processed = self.remove_background(tmp_path)
            tmp_path.unlink(missing_ok=True)
        elif fmt == ImageFormat.TIFF_MULTIPAGE:
            # Process only the first page
            original.seek(0)
            rgb_img = original.convert("RGB")
            tmp_path = file_path.with_suffix(".tmp.png")
            rgb_img.save(tmp_path, "PNG")
            processed = self.remove_background(tmp_path)
            tmp_path.unlink(missing_ok=True)
        else:
            processed = self.remove_background(file_path)

        # Crop
        cropped = self.crop_to_content(processed, self._config.crop_padding_px)

        # Confidence
        confidence = self.calculate_confidence(original, cropped)

        original.close()
        return cropped, fmt, confidence
