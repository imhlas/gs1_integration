"""JPG image processing — spec v5 vastuunjako.

``JpgProcessor`` owns:
- Background removal + crop for JPEG images

It does NOT handle TIFF logic or uploads.
Calls only ``ImageService``.
"""

from __future__ import annotations

import logging
from pathlib import Path

from PIL import Image

from sharepoint_image_processor.core.models import ImageFormat
from sharepoint_image_processor.services.image_service import ImageService

logger = logging.getLogger(__name__)


class JpgProcessor:
    """Process a single JPEG image: remove background → crop → score."""

    def __init__(self, image_service: ImageService) -> None:
        self._svc = image_service

    def process(
        self, file_path: Path, fmt_override: ImageFormat | None = None
    ) -> tuple[Image.Image, ImageFormat, float]:
        """Run rembg + crop on a JPEG or PNG file.

        Returns ``(processed_rgba, format, confidence)``.
        """
        fmt = fmt_override or ImageFormat.JPEG
        logger.debug("%s processing: %s", fmt.value.upper(), file_path.name)
        original = Image.open(file_path)

        processed = self._svc.remove_background(file_path)
        cropped = self._svc.crop_to_content(processed, self._svc._config.crop_padding_px)
        confidence = self._svc.calculate_confidence(original, cropped)

        original.close()
        return cropped, fmt, confidence
