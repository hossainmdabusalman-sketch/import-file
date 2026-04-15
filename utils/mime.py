"""
Filename → MIME-type mapping.

Keeping this in its own module makes it trivial to extend and unit-test.
"""
from __future__ import annotations

_EXT_MAP: dict[str, str] = {
    ".pdf":  "application/pdf",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".docx": (
        "application/vnd.openxmlformats-officedocument"
        ".wordprocessingml.document"
    ),
    ".xlsx": (
        "application/vnd.openxmlformats-officedocument"
        ".spreadsheetml.sheet"
    ),
    ".txt":  "text/plain",
    ".csv":  "text/csv",
    ".json": "application/json",
    ".zip":  "application/zip",
}

_DEFAULT_MIME = "application/octet-stream"


def content_type_for(filename: str) -> str:
    """Return the MIME type for *filename* based on its extension."""
    ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return _EXT_MAP.get(ext, _DEFAULT_MIME)