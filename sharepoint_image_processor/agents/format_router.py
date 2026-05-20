"""Format detection and routing to the correct processor.

Spec v5 — ``format_router.py`` owns:
- Format identification (delegates to ``image_service.detect_format``)
- Routing to ``JpgProcessor`` or ``TiffProcessor``

It does NOT perform image processing itself.
"""

from __future__ import annotations

import logging
from pathlib import Path

from PIL import Image

from sharepoint_image_processor.core.models import ImageFormat
from sharepoint_image_processor.services.image_service import ImageService

logger = logging.getLogger(__name__)


class FormatRouter:
    """Detect image format and delegate processing to the right processor."""

    def __init__(self, image_service: ImageService) -> None:
        self._image_service = image_service

    def detect(self, file_path: Path) -> ImageFormat:
        """Return the detected ``ImageFormat`` for *file_path*."""
        return self._image_service.detect_format(file_path)

    def process(self, file_path: Path) -> tuple[Image.Image, ImageFormat, float]:
        """Route *file_path* through the appropriate processor pipeline.

        Delegates to ``JpgProcessor`` or ``TiffProcessor`` depending on
        the detected format.  Returns ``(processed_image, format, confidence)``.
        """
        from sharepoint_image_processor.agents.jpg_processor import JpgProcessor
        from sharepoint_image_processor.agents.tiff_processor import TiffProcessor

        fmt = self.detect(file_path)

        if fmt == ImageFormat.JPEG:
            return JpgProcessor(self._image_service).process(file_path)
        if fmt == ImageFormat.PNG:
            # PNG with white/solid background — treat same as JPEG
            return JpgProcessor(self._image_service).process(file_path, fmt_override=ImageFormat.PNG)
        if fmt in (ImageFormat.TIFF_RGB, ImageFormat.TIFF_CMYK, ImageFormat.TIFF_MULTIPAGE):
            return TiffProcessor(self._image_service).process(file_path, fmt)

        raise ValueError(f"Unsupported format for {file_path.name}: {fmt}")
