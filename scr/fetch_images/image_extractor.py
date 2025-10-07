# image_extractor.py
from dataclasses import dataclass
import mimetypes
import requests


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
    extension: str  # esim. ".jpg"


class ImageExtractor:
    def __init__(self, timeout_sec: int = 30):
        self.timeout_sec = timeout_sec
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
        Hakee kuvan URL:ista ja palauttaa ImageBlobin.
        Heittää poikkeuksen, jos HTTP-pyyntö epäonnistuu.
        """
        r = self.session.get(item.url, timeout=self.timeout_sec, stream=True)
        r.raise_for_status()
        content = r.content
        ext = self._guess_extension(r, item.url)
        return ImageBlob(
            gtin=item.gtin,
            gpc_family_code=item.gpc_family_code,
            content=content,
            extension=ext,
        )
