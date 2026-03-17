# src/fetch_images/image_extractor.py
from dataclasses import dataclass
from src.config import *
import mimetypes
import requests

try:
    from src.fetch_images.remove_background import remove_background, is_available as rembg_available
except Exception:
    # rembg ei asennettu — taustan poisto ohitetaan
    def remove_background(b): return b
    def rembg_available(): return False


@dataclass(frozen=True)
class ItemImage:
    gpc_family_code: str
    gtin: str
    url: str


@dataclass(frozen=True)
class ImageBlob:
    gtin: str
    gpc_family_code: str
    content: bytes
    extension: str  # esim. ".jpg", ".png"


class ImageExtractor:
    def __init__(self, timeout_sec: int = 30, remove_bg: bool = True):
        self.timeout_sec = timeout_sec
        self.remove_bg = remove_bg
        self.session = requests.Session()

    def _guess_extension(self, resp: requests.Response, url: str) -> str:
        ctype = (resp.headers.get("Content-Type") or "").split(";", 1)[0].strip()
        ext = mimetypes.guess_extension(ctype) if ctype else None
        if not ext:
            path = requests.utils.urlparse(url).path.lower()
            for cand in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"):
                if path.endswith(cand):
                    ext = cand
                    break
        return ext or ".jpg"  # yksinkertainen oletus

    def fetch(self, item: ItemImage) -> ImageBlob:
        """
        Hakee kuvan URL:ista, poistaa taustan (jos rembg saatavilla)
        ja palauttaa ImageBlobin.
        Heittää poikkeuksen, jos HTTP-pyyntö epäonnistuu.
        """
        r = self.session.get(item.url, timeout=self.timeout_sec, stream=True)
        r.raise_for_status()
        content = r.content
        ext = self._guess_extension(r, item.url)

        # Taustan poisto: tuottaa PNG:n transparentilla taustalla
        if self.remove_bg and rembg_available():
            content = remove_background(content)
            ext = ".png"  # rembg palauttaa aina PNG:n

        return ImageBlob(
            gtin=item.gtin,
            gpc_family_code=item.gpc_family_code,
            content=content,
            extension=ext,
        )
