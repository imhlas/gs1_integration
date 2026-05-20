"""TIFF image processing — spec v5 vastuunjako.

``TiffProcessor`` owns:
- CMYK → RGB conversion
- Multi-page TIFF handling (first page only)
- Background removal + crop for TIFF images

It does NOT handle JPG logic or uploads.
Calls only ``ImageService``.
"""

from __future__ import annotations

import logging
from pathlib import Path

from PIL import Image

from sharepoint_image_processor.core.models import ImageFormat
from sharepoint_image_processor.services.image_service import ImageService

logger = logging.getLogger(__name__)


class TiffProcessor:
    """Process a single TIFF image through the appropriate sub-pipeline."""

    def __init__(self, image_service: ImageService) -> None:
        self._svc = image_service

    def process(
        self, file_path: Path, fmt: ImageFormat
    ) -> tuple[Image.Image, ImageFormat, float]:
        """Handle TIFF_RGB, TIFF_CMYK, or TIFF_MULTIPAGE.

        Returns ``(processed_rgba, detected_format, confidence)``.
        """
        logger.debug("TIFF processing (%s): %s", fmt.value, file_path.name)
        original = Image.open(file_path)

        if fmt == ImageFormat.TIFF_CMYK:
            return self._process_cmyk(file_path, original, fmt)
        if fmt == ImageFormat.TIFF_MULTIPAGE:
            return self._process_multipage(file_path, original, fmt)
        # TIFF_RGB — treat like a normal image
        return self._process_rgb(file_path, original, fmt)

    def _process_rgb(
        self, file_path: Path, original: Image.Image, fmt: ImageFormat
    ) -> tuple[Image.Image, ImageFormat, float]:
        processed = self._svc.remove_background(file_path)
        cropped = self._svc.crop_to_content(processed, self._svc._config.crop_padding_px)
        confidence = self._svc.calculate_confidence(original, cropped)
        original.close()
        return cropped, fmt, confidence

    def _process_cmyk(
        self, file_path: Path, original: Image.Image, fmt: ImageFormat
    ) -> tuple[Image.Image, ImageFormat, float]:
        """CMYK → RGB → rembg → crop."""
        rgb_img = self._svc.convert_cmyk_to_rgb(original)
        tmp_path = file_path.with_suffix(".tmp.png")
        try:
            rgb_img.save(tmp_path, "PNG")
            processed = self._svc.remove_background(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        cropped = self._svc.crop_to_content(processed, self._svc._config.crop_padding_px)
        confidence = self._svc.calculate_confidence(original, cropped)
        original.close()
        return cropped, fmt, confidence

    def _process_multipage(
        self, file_path: Path, original: Image.Image, fmt: ImageFormat
    ) -> tuple[Image.Image, ImageFormat, float]:
        """Multi-page TIFF: extract first page → RGB → rembg → crop."""
        original.seek(0)
        first_page = original.convert("RGB")
        tmp_path = file_path.with_suffix(".tmp.png")
        try:
            first_page.save(tmp_path, "PNG")
            processed = self._svc.remove_background(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        cropped = self._svc.crop_to_content(processed, self._svc._config.crop_padding_px)
        confidence = self._svc.calculate_confidence(original, cropped)
        original.close()
        return cropped, fmt, confidence
