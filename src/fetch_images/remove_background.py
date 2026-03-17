# src/fetch_images/remove_background.py
# Automaattinen taustan poisto tuotekuvista hyllykuvia varten.
# Käyttää rembg-kirjastoa (U2NET-malli).
#
# Asennus Databricksissa:
#   %pip install rembg onnxruntime
#
# Paikallisesti:
#   pip install rembg[cpu] onnxruntime

from io import BytesIO

_rembg_available = None
_rembg_session = None


def _ensure_rembg():
    """Lazy-load rembg. Returns True if available, False otherwise."""
    global _rembg_available, _rembg_session
    if _rembg_available is not None:
        return _rembg_available
    try:
        from rembg import new_session
        _rembg_session = new_session(model_name="u2net")
        _rembg_available = True
    except ImportError:
        print("[remove_background] rembg not installed. Background removal disabled.")
        print("  Install with: pip install rembg[cpu] onnxruntime")
        _rembg_available = False
    except Exception as e:
        print(f"[remove_background] Failed to initialize rembg: {e}")
        _rembg_available = False
    return _rembg_available


def remove_background(image_bytes: bytes) -> bytes:
    """Remove the background from a product image.

    Args:
        image_bytes: Raw image file bytes (JPEG, PNG, etc.)

    Returns:
        PNG bytes with transparent background.
        If rembg is not available, returns the original bytes unchanged.
    """
    if not _ensure_rembg():
        return image_bytes

    from rembg import remove
    result = remove(image_bytes, session=_rembg_session)
    return result


def is_available() -> bool:
    """Check if background removal is available."""
    return _ensure_rembg()
