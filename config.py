"""
Central configuration – all tuneable constants live here.
Override any value via environment variables if needed.
"""
from __future__ import annotations

import os

DEFAULT_ATTACH_FIELD: str = "file_attachment"
DEFAULT_MAX_WORKERS: int = 20
DEFAULT_FILE_COL_GUESSES: list[str] = [
    "related_file", "file_name", "filename", "attachment", "file"
]

HTTP_POOL_CONNECTIONS: int = 20
HTTP_POOL_MAXSIZE: int = 20
HTTP_TIMEOUT_RECORD: int = 30   
HTTP_TIMEOUT_FILE: int = 60     

MAX_RETRIES: int = 3
RETRY_BACKOFF_BASE: float = 0.5
RETRY_BACKOFF_429_MULTIPLIER: int = 3

MAX_FORM_FILES: int = 2_500

CORS_ALLOW_ORIGINS: list[str] = ["*"]
CORS_ALLOW_METHODS: list[str] = ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
CORS_ALLOW_HEADERS: list[str] = ["*"]

STATIC_DIR: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")