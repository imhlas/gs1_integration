"""Metadata whitelist, required-field validation, and PATCH payload builder.

Spec v5 sections 5 and 9: the three public functions form the complete
metadata pipeline used by ``UploadAgent.copy_metadata``.

Field lists are derived from the "Metadata-sarakkeet" table in the spec.
"""

from datetime import datetime, timezone


# ── Field lists (derived from spec metadata table) ───────────────────

COPYABLE_FIELDS: list[str] = [
    "Title",                     # Spec "Otsikko" — SharePoint internal name is Title
    "_ExtendedDescription",      # Spec "Kuvaus"  — SharePoint internal name is _ExtendedDescription
    "EAN",
    "Kesko_x002d_kategoria1",
    "Kesko_x002d_kategoria2",
    "Kesko_x002d_kategoria3",
    "GS1_x002d_kategoria1",
    "GS1_x002d_kategoria2",
    "BRAND",
    "Tuote",
]

REQUIRED_FIELDS: list[str] = [
    "EAN",
    "BRAND",
    "Tuote",
]

SKIP_FIELDS: list[str] = [
    "MediaServiceImageTags",     # Managed metadata (Image Tags) — not copyable
    "Modified",                  # Read-only system field
    "Created",                   # Read-only system field
    "Author",                    # Read-only person field (Created By)
    "Editor",                    # Read-only person field (Modified By)
    "CheckoutUser",              # Read-only person field (Checked Out To)
]

ADDED_FIELDS_TEMPLATE: dict = {
    "Processed": True,
    "ProcessedDate": None,       # Filled at build time
    "OriginalItemId": None,      # Filled at build time
}


# ── Public functions ─────────────────────────────────────────────────

def extract_copyable_fields(raw_fields: dict) -> dict:
    """Return only the whitelisted fields from a raw ``listItem.fields`` dict.

    Fields present in ``COPYABLE_FIELDS`` are kept (including those whose
    value is ``None``).  Everything else is dropped.
    """
    return {key: raw_fields[key] for key in COPYABLE_FIELDS if key in raw_fields}


def validate_required_fields(fields: dict) -> tuple[bool, list[str]]:
    """Check that all required fields are present and non-empty.

    Returns:
        ``(True, [])`` when all required fields are satisfied, or
        ``(False, ["EAN", ...])`` listing the missing / empty ones.
    """
    missing: list[str] = []
    for field_name in REQUIRED_FIELDS:
        value = fields.get(field_name)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            missing.append(field_name)
    return (len(missing) == 0, missing)


def build_patch_payload(copyable: dict, original_item_id: str) -> dict:
    """Merge copyable fields with the processor-added fields.

    The resulting dict is ready to be sent as the JSON body of a
    ``PATCH /items/{id}/listItem/fields`` request.
    """
    payload = dict(copyable)
    payload["Processed"] = True
    payload["ProcessedDate"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload["OriginalItemId"] = original_item_id
    return payload
