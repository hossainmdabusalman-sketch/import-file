"""
Core upload worker.

``process_row`` is executed inside a ``ThreadPoolExecutor``.  It:
  1. Creates a ServiceNow record.
  2. Uploads the binary attachment.
  3. Links the attachment to the record via PATCH.
  4. Optionally hides the attachment from the form's attachment banner.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict

import requests

from config import (
    HTTP_TIMEOUT_FILE,
    HTTP_TIMEOUT_RECORD,
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
    RETRY_BACKOFF_429_MULTIPLIER,
)
from services.job_store import store
from services.session import get_session
from utils.mime import content_type_for


# ── Parameter bundle ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class UploadContext:
    """Immutable bundle of per-job configuration passed to every worker."""
    job_id: str
    instance: str          # e.g. "https://myinstance.service-now.com"
    username: str
    password: str
    table_name: str
    files_map: Dict[str, bytes]    # filename → raw bytes
    field_mappings: Dict[str, str] # csv_column → snow_field
    file_col: str                  # CSV column that holds the attachment name
    attach_field: str              # ServiceNow field for the attachment ref


# ── ServiceNow API helpers ────────────────────────────────────────────────────

def _create_record(session: requests.Session, base: str, table: str, payload: dict) -> str:
    """POST a new record; return its sys_id."""
    url = f"{base}/api/now/table/{table}"
    resp = session.post(url, json=payload, timeout=HTTP_TIMEOUT_RECORD)
    resp.raise_for_status()
    return resp.json()["result"]["sys_id"]


def _upload_attachment(
    session: requests.Session,
    base: str,
    table: str,
    record_sys_id: str,
    file_name: str,
    file_bytes: bytes,
) -> str:
    """POST binary file to the attachment API; return the attachment sys_id."""
    url = (
        f"{base}/api/now/attachment/file"
        f"?table_name={table}&table_sys_id={record_sys_id}&file_name={file_name}"
    )
    resp = session.post(
        url,
        data=file_bytes,
        headers={"Content-Type": content_type_for(file_name)},
        timeout=HTTP_TIMEOUT_FILE,
    )
    resp.raise_for_status()
    return resp.json()["result"]["sys_id"]


def _link_attachment(
    session: requests.Session,
    base: str,
    table: str,
    record_sys_id: str,
    attach_field: str,
    attachment_sys_id: str,
) -> None:
    """PATCH the record to set the file-reference field."""
    url = f"{base}/api/now/table/{table}/{record_sys_id}"
    resp = session.patch(
        url,
        json={attach_field: attachment_sys_id},
        timeout=HTTP_TIMEOUT_RECORD,
    )
    resp.raise_for_status()


def _hide_attachment_from_banner(
    session: requests.Session,
    base: str,
    attachment_sys_id: str,
    table: str,
) -> None:
    """
    Rename the attachment's table_name so it won't appear in the form's
    'Manage Attachments' banner while still being reachable via the field ref.
    """
    url = f"{base}/api/now/table/sys_attachment/{attachment_sys_id}"
    try:
        session.patch(
            url,
            json={"table_name": f"{table}_hidden"},
            timeout=HTTP_TIMEOUT_RECORD,
        )
    except Exception:
        pass


def _backoff(attempt: int, status_code: int) -> float:
    multiplier = RETRY_BACKOFF_429_MULTIPLIER if status_code == 429 else 1
    return RETRY_BACKOFF_BASE * multiplier * (2 ** (attempt - 1))


def process_row(row: dict, index: int, ctx: UploadContext) -> None:
    """
    Process a single CSV row: create a record, upload its attachment, link them.

    Outcomes are written directly to the shared ``JobStore``.
    """
    file_name = row.get(ctx.file_col, "").strip()

    if file_name not in ctx.files_map:
        store.record_outcome(
            ctx.job_id, "skipped",
            f"SKIP [{index}] '{file_name}' — not found in uploaded files",
        )
        return

    file_bytes = ctx.files_map[file_name]

    record_payload = {
        snow_field: row[csv_col].strip()
        for csv_col, snow_field in ctx.field_mappings.items()
        if csv_col in row and snow_field != ctx.attach_field
    }

    base = ctx.instance.rstrip("/")
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = get_session(ctx.username, ctx.password)

            record_sys_id     = _create_record(session, base, ctx.table_name, record_payload)
            attachment_sys_id = _upload_attachment(session, base, ctx.table_name, record_sys_id, file_name, file_bytes)
            _link_attachment(session, base, ctx.table_name, record_sys_id, ctx.attach_field, attachment_sys_id)
            _hide_attachment_from_banner(session, base, attachment_sys_id, ctx.table_name)

            store.record_outcome(
                ctx.job_id, "success",
                f"OK  [{index}] '{file_name}'",
                bytes_uploaded=len(file_bytes),
            )
            return

        except requests.HTTPError as exc:
            last_exc = exc
            status = exc.response.status_code if exc.response is not None else 0
            if attempt < MAX_RETRIES:
                time.sleep(_backoff(attempt, status))

        except Exception as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_BASE * attempt)

    store.record_outcome(
        ctx.job_id, "failed",
        f"FAIL [{index}] '{file_name}' — {last_exc}",
    )