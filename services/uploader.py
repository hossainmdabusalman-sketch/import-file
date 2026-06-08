# """
# Core upload worker.

# ``process_row`` is executed inside a ``ThreadPoolExecutor``.  It:
#   1. Creates a ServiceNow record.
#   2. Uploads the binary attachment.
#   3. Links the attachment to the record via PATCH.
#   4. Optionally hides the attachment from the form's attachment banner.
# """
# from __future__ import annotations

# import time
# from dataclasses import dataclass
# from typing import Dict

# import requests

# from config import (
#     HTTP_TIMEOUT_FILE,
#     HTTP_TIMEOUT_RECORD,
#     MAX_RETRIES,
#     RETRY_BACKOFF_BASE,
#     RETRY_BACKOFF_429_MULTIPLIER,
# )
# from services.job_store import store
# from services.session import get_session
# from utils.mime import content_type_for


# # ── Parameter bundle ───────────────────────────────────────────────────────────

# @dataclass(frozen=True)
# class UploadContext:
#     """Immutable bundle of per-job configuration passed to every worker."""
#     job_id: str
#     instance: str          # e.g. "https://myinstance.service-now.com"
#     username: str
#     password: str
#     table_name: str
#     files_map: Dict[str, bytes]    # filename → raw bytes
#     field_mappings: Dict[str, str] # csv_column → snow_field
#     file_col: str                  # CSV column that holds the attachment name
#     attach_field: str              # ServiceNow field for the attachment ref


# # ── ServiceNow API helpers ────────────────────────────────────────────────────

# def _create_record(session: requests.Session, base: str, table: str, payload: dict) -> str:
#     """POST a new record; return its sys_id."""
#     url = f"{base}/api/now/table/{table}"
#     resp = session.post(url, json=payload, timeout=HTTP_TIMEOUT_RECORD)
#     resp.raise_for_status()
#     return resp.json()["result"]["sys_id"]


# def _upload_attachment(
#     session: requests.Session,
#     base: str,
#     table: str,
#     record_sys_id: str,
#     file_name: str,
#     file_bytes: bytes,
# ) -> str:
#     """POST binary file to the attachment API; return the attachment sys_id."""
#     url = (
#         f"{base}/api/now/attachment/file"
#         f"?table_name={table}&table_sys_id={record_sys_id}&file_name={file_name}"
#     )
#     resp = session.post(
#         url,
#         data=file_bytes,
#         headers={"Content-Type": content_type_for(file_name)},
#         timeout=HTTP_TIMEOUT_FILE,
#     )
#     resp.raise_for_status()
#     return resp.json()["result"]["sys_id"]


# def _link_attachment(
#     session: requests.Session,
#     base: str,
#     table: str,
#     record_sys_id: str,
#     attach_field: str,
#     attachment_sys_id: str,
# ) -> None:
#     """PATCH the record to set the file-reference field."""
#     url = f"{base}/api/now/table/{table}/{record_sys_id}"
#     resp = session.patch(
#         url,
#         json={attach_field: attachment_sys_id},
#         timeout=HTTP_TIMEOUT_RECORD,
#     )
#     resp.raise_for_status()


# def _hide_attachment_from_banner(
#     session: requests.Session,
#     base: str,
#     attachment_sys_id: str,
#     table: str,
# ) -> None:
#     """
#     Rename the attachment's table_name so it won't appear in the form's
#     'Manage Attachments' banner while still being reachable via the field ref.
#     """
#     url = f"{base}/api/now/table/sys_attachment/{attachment_sys_id}"
#     try:
#         session.patch(
#             url,
#             json={"table_name": f"{table}_hidden"},
#             timeout=HTTP_TIMEOUT_RECORD,
#         )
#     except Exception:
#         pass


# def _backoff(attempt: int, status_code: int) -> float:
#     multiplier = RETRY_BACKOFF_429_MULTIPLIER if status_code == 429 else 1
#     return RETRY_BACKOFF_BASE * multiplier * (2 ** (attempt - 1))


# def process_row(row: dict, index: int, ctx: UploadContext) -> None:
#     """
#     Process a single CSV row: create a record, upload its attachment, link them.

#     Outcomes are written directly to the shared ``JobStore``.
#     """
#     file_name = row.get(ctx.file_col, "").strip()

#     if file_name not in ctx.files_map:
#         store.record_outcome(
#             ctx.job_id, "skipped",
#             f"SKIP [{index}] '{file_name}' — not found in uploaded files",
#         )
#         return

#     file_bytes = ctx.files_map[file_name]

#     record_payload = {
#         snow_field: row[csv_col].strip()
#         for csv_col, snow_field in ctx.field_mappings.items()
#         if csv_col in row and snow_field != ctx.attach_field
#     }

#     base = ctx.instance.rstrip("/")
#     last_exc: Exception | None = None

#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             session = get_session(ctx.username, ctx.password)

#             record_sys_id     = _create_record(session, base, ctx.table_name, record_payload)
#             attachment_sys_id = _upload_attachment(session, base, ctx.table_name, record_sys_id, file_name, file_bytes)
#             _link_attachment(session, base, ctx.table_name, record_sys_id, ctx.attach_field, attachment_sys_id)
#             _hide_attachment_from_banner(session, base, attachment_sys_id, ctx.table_name)

#             store.record_outcome(
#                 ctx.job_id, "success",
#                 f"OK  [{index}] '{file_name}'",
#                 bytes_uploaded=len(file_bytes),
#             )
#             return

#         except requests.HTTPError as exc:
#             last_exc = exc
#             status = exc.response.status_code if exc.response is not None else 0
#             if attempt < MAX_RETRIES:
#                 time.sleep(_backoff(attempt, status))

#         except Exception as exc:
#             last_exc = exc
#             if attempt < MAX_RETRIES:
#                 time.sleep(RETRY_BACKOFF_BASE * attempt)

#     store.record_outcome(
#         ctx.job_id, "failed",
#         f"FAIL [{index}] '{file_name}' — {last_exc}",
#     )



# """
# Core upload worker.

# ``process_row`` is executed inside a ``ThreadPoolExecutor``.  It:
#   1. Creates a ServiceNow record.
#   2. Uploads the binary attachment.
#   3. Links the attachment to the record via PATCH.
#   4. Optionally hides the attachment from the form's attachment banner.
# """
# from __future__ import annotations

# import time
# from dataclasses import dataclass
# from typing import Dict

# import requests

# from config import (
#     HTTP_TIMEOUT_FILE,
#     HTTP_TIMEOUT_RECORD,
#     MAX_RETRIES,
#     RETRY_BACKOFF_BASE,
#     RETRY_BACKOFF_429_MULTIPLIER,
# )
# from services.job_store import store
# from services.session import get_session
# from utils.mime import content_type_for


# # ── Parameter bundle ───────────────────────────────────────────────────────────

# @dataclass(frozen=True)
# class UploadContext:
#     """Immutable bundle of per-job configuration passed to every worker."""
#     job_id: str
#     instance: str           # e.g. "https://myinstance.service-now.com"
#     username: str
#     password: str
#     table_name: str
#     files_map: Dict[str, bytes]     # "folder/filename" → raw bytes
#     field_mappings: Dict[str, str]  # csv_column → snow_field
#     file_col: str                   # CSV column that holds the attachment filename
#     file_path_col: str              # CSV column that holds the subfolder/key (e.g. col1_logical_key)
#     attach_field: str               # ServiceNow field for the attachment ref


# # ── ServiceNow API helpers ────────────────────────────────────────────────────

# def _create_record(session: requests.Session, base: str, table: str, payload: dict) -> str:
#     """POST a new record; return its sys_id."""
#     url = f"{base}/api/now/table/{table}"
#     resp = session.post(url, json=payload, timeout=HTTP_TIMEOUT_RECORD)
#     resp.raise_for_status()
#     return resp.json()["result"]["sys_id"]


# def _upload_attachment(
#     session: requests.Session,
#     base: str,
#     table: str,
#     record_sys_id: str,
#     file_name: str,
#     file_bytes: bytes,
# ) -> str:
#     """POST binary file to the attachment API; return the attachment sys_id."""
#     url = (
#         f"{base}/api/now/attachment/file"
#         f"?table_name={table}&table_sys_id={record_sys_id}&file_name={file_name}"
#     )
#     resp = session.post(
#         url,
#         data=file_bytes,
#         headers={"Content-Type": content_type_for(file_name)},
#         timeout=HTTP_TIMEOUT_FILE,
#     )
#     resp.raise_for_status()
#     return resp.json()["result"]["sys_id"]


# def _link_attachment(
#     session: requests.Session,
#     base: str,
#     table: str,
#     record_sys_id: str,
#     attach_field: str,
#     attachment_sys_id: str,
# ) -> None:
#     """PATCH the record to set the file-reference field."""
#     url = f"{base}/api/now/table/{table}/{record_sys_id}"
#     resp = session.patch(
#         url,
#         json={attach_field: attachment_sys_id},
#         timeout=HTTP_TIMEOUT_RECORD,
#     )
#     resp.raise_for_status()


# def _hide_attachment_from_banner(
#     session: requests.Session,
#     base: str,
#     attachment_sys_id: str,
#     table: str,
# ) -> None:
#     """
#     Rename the attachment's table_name so it won't appear in the form's
#     'Manage Attachments' banner while still being reachable via the field ref.
#     """
#     url = f"{base}/api/now/table/sys_attachment/{attachment_sys_id}"
#     try:
#         session.patch(
#             url,
#             json={"table_name": f"{table}_hidden"},
#             timeout=HTTP_TIMEOUT_RECORD,
#         )
#     except Exception:
#         pass


# def _backoff(attempt: int, status_code: int) -> float:
#     multiplier = RETRY_BACKOFF_429_MULTIPLIER if status_code == 429 else 1
#     return RETRY_BACKOFF_BASE * multiplier * (2 ** (attempt - 1))


# def process_row(row: dict, index: int, ctx: UploadContext) -> None:
#     """
#     Process a single CSV row: create a record, upload its attachment, link them.

#     The attachment is looked up by combining the folder column and filename column:
#         e.g. col1_logical_key="c", col3_file_attachment="b.pdf" → lookup_key="c/b.pdf"

#     This allows multiple files with the same filename in different subfolders
#     to be uniquely identified.

#     Outcomes are written directly to the shared ``JobStore``.
#     """
#     file_name   = row.get(ctx.file_col, "").strip()       # e.g. "b.pdf"
#     folder_name = row.get(ctx.file_path_col, "").strip()  # e.g. "c"

#     # Composite key: "c/b.pdf" — matches how files_map is built in upload.py
#     lookup_key = f"{folder_name}/{file_name}" if folder_name else file_name

#     if lookup_key not in ctx.files_map:
#         store.record_outcome(
#             ctx.job_id, "skipped",
#             f"SKIP [{index}] '{lookup_key}' — not found in uploaded files",
#         )
#         return

#     file_bytes = ctx.files_map[lookup_key]

#     # Build the record payload, excluding the attach field itself
#     record_payload = {
#         snow_field: row[csv_col].strip()
#         for csv_col, snow_field in ctx.field_mappings.items()
#         if csv_col in row and snow_field != ctx.attach_field
#     }

#     base = ctx.instance.rstrip("/")
#     last_exc: Exception | None = None

#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             session = get_session(ctx.username, ctx.password)

#             record_sys_id = _create_record(session, base, ctx.table_name, record_payload)

#             # Use lookup_key ("c/b.pdf") as the stored filename in ServiceNow
#             # so the File Name column shows the full path, not just "b.pdf"
#             attachment_sys_id = _upload_attachment(
#                 session, base, ctx.table_name, record_sys_id,
#                 lookup_key,   # stored as "c/b.pdf" in ServiceNow File Name column
#                 file_bytes,
#             )

#             _link_attachment(
#                 session, base, ctx.table_name, record_sys_id,
#                 ctx.attach_field, attachment_sys_id,
#             )
#             _hide_attachment_from_banner(session, base, attachment_sys_id, ctx.table_name)

#             store.record_outcome(
#                 ctx.job_id, "success",
#                 f"OK  [{index}] '{lookup_key}'",
#                 bytes_uploaded=len(file_bytes),
#             )
#             return

#         except requests.HTTPError as exc:
#             last_exc = exc
#             status = exc.response.status_code if exc.response is not None else 0
#             if attempt < MAX_RETRIES:
#                 time.sleep(_backoff(attempt, status))

#         except Exception as exc:
#             last_exc = exc
#             if attempt < MAX_RETRIES:
#                 time.sleep(RETRY_BACKOFF_BASE * attempt)

#     store.record_outcome(
#         ctx.job_id, "failed",
#         f"FAIL [{index}] '{lookup_key}' — {last_exc}",
#     )


# """
# Core upload worker.

# ``process_row`` is executed inside a ``ThreadPoolExecutor``.  It:
#   1. Creates a ServiceNow record.
#   2. Uploads the binary attachment.
#   3. Links the attachment to the record via PATCH.
#   4. Optionally hides the attachment from the form's attachment banner.

# Lookup strategy
# ---------------
# The browser may send files with or without subfolder paths depending on the
# folder structure the user selected.

#   Nested:  masterfolder/a/a.pdf  →  files_map key = "a/a.pdf"
#   Flat:    masterfolder/a.pdf    →  files_map key = "a.pdf"

# In both cases we try the composite key first, then the bare filename.
# The filename stored in ServiceNow is ALWAYS the composite "folder/file.pdf"
# so the File Name column is consistent regardless of the local folder layout.
# """
# from __future__ import annotations

# import time
# from dataclasses import dataclass
# from typing import Dict, Optional

# import requests

# from config import (
#     HTTP_TIMEOUT_FILE,
#     HTTP_TIMEOUT_RECORD,
#     MAX_RETRIES,
#     RETRY_BACKOFF_BASE,
#     RETRY_BACKOFF_429_MULTIPLIER,
# )
# from services.job_store import store
# from services.session import get_session
# from utils.mime import content_type_for


# # ── Parameter bundle ───────────────────────────────────────────────────────────

# @dataclass(frozen=True)
# class UploadContext:
#     """Immutable bundle of per-job configuration passed to every worker."""
#     job_id: str
#     instance: str
#     username: str
#     password: str
#     table_name: str
#     files_map: Dict[str, bytes]     # key → raw bytes
#     field_mappings: Dict[str, str]  # csv_column → snow_field
#     file_col: str                   # CSV column: attachment filename  e.g. "u_file_name"
#     file_path_col: str              # CSV column: subfolder/key        e.g. "u_folder_name"
#     attach_field: str               # ServiceNow field for the attachment ref


# # ── ServiceNow API helpers ────────────────────────────────────────────────────

# def _create_record(session: requests.Session, base: str, table: str, payload: dict) -> str:
#     url = f"{base}/api/now/table/{table}"
#     resp = session.post(url, json=payload, timeout=HTTP_TIMEOUT_RECORD)
#     resp.raise_for_status()
#     return resp.json()["result"]["sys_id"]


# def _upload_attachment(
#     session: requests.Session,
#     base: str,
#     table: str,
#     record_sys_id: str,
#     file_name: str,
#     file_bytes: bytes,
# ) -> str:
#     url = (
#         f"{base}/api/now/attachment/file"
#         f"?table_name={table}&table_sys_id={record_sys_id}&file_name={file_name}"
#     )
#     resp = session.post(
#         url,
#         data=file_bytes,
#         headers={"Content-Type": content_type_for(file_name)},
#         timeout=HTTP_TIMEOUT_FILE,
#     )
#     resp.raise_for_status()
#     return resp.json()["result"]["sys_id"]


# def _link_attachment(
#     session: requests.Session,
#     base: str,
#     table: str,
#     record_sys_id: str,
#     attach_field: str,
#     attachment_sys_id: str,
# ) -> None:
#     url = f"{base}/api/now/table/{table}/{record_sys_id}"
#     resp = session.patch(
#         url,
#         json={attach_field: attachment_sys_id},
#         timeout=HTTP_TIMEOUT_RECORD,
#     )
#     resp.raise_for_status()


# def _hide_attachment_from_banner(
#     session: requests.Session,
#     base: str,
#     attachment_sys_id: str,
#     table: str,
# ) -> None:
#     url = f"{base}/api/now/table/sys_attachment/{attachment_sys_id}"
#     try:
#         session.patch(
#             url,
#             json={"table_name": f"{table}_hidden"},
#             timeout=HTTP_TIMEOUT_RECORD,
#         )
#     except Exception:
#         pass


# def _backoff(attempt: int, status_code: int) -> float:
#     multiplier = RETRY_BACKOFF_429_MULTIPLIER if status_code == 429 else 1
#     return RETRY_BACKOFF_BASE * multiplier * (2 ** (attempt - 1))


# def _find_file_bytes(
#     files_map: Dict[str, bytes],
#     folder_name: str,
#     file_name: str,
# ) -> Optional[bytes]:
#     """
#     Locate file bytes in files_map using two strategies:

#     1. Composite key  "folder/file.pdf"  — nested folder structure
#     2. Bare filename  "file.pdf"         — flat folder structure

#     Returns raw bytes if found, None otherwise.
#     """
#     # 1. Nested: "a/a.pdf"
#     if folder_name:
#         composite = f"{folder_name}/{file_name}"
#         if composite in files_map:
#             return files_map[composite]

#     # 2. Flat: "a.pdf"
#     if file_name in files_map:
#         return files_map[file_name]

#     return None


# def process_row(row: dict, index: int, ctx: UploadContext) -> None:
#     """
#     Process a single CSV row: create record → upload attachment → link → hide banner.

#     The attachment is always stored in ServiceNow as "folder/file.pdf"
#     (e.g. "a/a.pdf") regardless of whether the local files were nested or flat.
#     This keeps the ServiceNow File Name column consistent.
#     """
#     file_name   = row.get(ctx.file_col,      "").strip()   # "a.pdf"
#     folder_name = row.get(ctx.file_path_col, "").strip()   # "a"

#     # Always use the composite as the ServiceNow filename
#     sn_file_name = f"{folder_name}/{file_name}" if folder_name else file_name
#     # e.g. "a/a.pdf" — what gets stored in the SN File Name column

#     file_bytes = _find_file_bytes(ctx.files_map, folder_name, file_name)

#     if file_bytes is None:
#         tried = []
#         if folder_name:
#             tried.append(f"'{folder_name}/{file_name}'")
#         tried.append(f"'{file_name}'")
#         store.record_outcome(
#             ctx.job_id, "skipped",
#             f"SKIP [{index}] tried {', '.join(tried)} — not found in uploaded files",
#         )
#         return

#     # Build record payload — exclude the attachment field itself
#     record_payload = {
#         snow_field: row[csv_col].strip()
#         for csv_col, snow_field in ctx.field_mappings.items()
#         if csv_col in row and snow_field != ctx.attach_field
#     }

#     base = ctx.instance.rstrip("/")
#     last_exc: Exception | None = None

#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             session = get_session(ctx.username, ctx.password)

#             record_sys_id = _create_record(session, base, ctx.table_name, record_payload)

#             # sn_file_name = "a/a.pdf" regardless of flat vs nested local files
#             attachment_sys_id = _upload_attachment(
#                 session, base, ctx.table_name, record_sys_id,
#                 sn_file_name,
#                 file_bytes,
#             )

#             _link_attachment(
#                 session, base, ctx.table_name, record_sys_id,
#                 ctx.attach_field, attachment_sys_id,
#             )
#             _hide_attachment_from_banner(session, base, attachment_sys_id, ctx.table_name)

#             store.record_outcome(
#                 ctx.job_id, "success",
#                 f"OK  [{index}] '{sn_file_name}'",
#                 bytes_uploaded=len(file_bytes),
#             )
#             return

#         except requests.HTTPError as exc:
#             last_exc = exc
#             status = exc.response.status_code if exc.response is not None else 0
#             if attempt < MAX_RETRIES:
#                 time.sleep(_backoff(attempt, status))

#         except Exception as exc:
#             last_exc = exc
#             if attempt < MAX_RETRIES:
#                 time.sleep(RETRY_BACKOFF_BASE * attempt)

#     store.record_outcome(
#         ctx.job_id, "failed",
#         f"FAIL [{index}] '{sn_file_name}' — {last_exc}",
#     )


"""
Core upload worker.

``process_row`` is executed inside a ``ThreadPoolExecutor``.  It:
  1. Creates a ServiceNow record.
  2. Uploads the binary attachment.
  3. Links the attachment to the record via PATCH.
  4. Optionally hides the attachment from the form's attachment banner.

Lookup strategy
---------------
files_map is always keyed by the relative path AFTER stripping the root folder:

  Browser sends:  masterfolder/a/a.pdf   →  key = "a/a.pdf"   (nested)
  Browser sends:  masterfolder/a.pdf     →  key = "a.pdf"      (flat)

CSV rows always have both a folder column and a filename column:

  u_folder_name="a",  u_file_name="a.pdf"  →  composite lookup = "a/a.pdf"
  u_folder_name="c",  u_file_name="b.pdf"  →  composite lookup = "c/b.pdf"

The composite key is the ONLY lookup strategy when a folder column is present.
The bare-filename fallback is used ONLY when folder_name is genuinely empty,
meaning the CSV row has no folder column at all.  This prevents a row whose
composite key misses (e.g. typo in CSV) from silently picking up a same-named
file from a different subfolder.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional

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
    instance: str           # e.g. "https://myinstance.service-now.com"
    username: str
    password: str
    table_name: str
    files_map: Dict[str, bytes]     # "folder/filename" or "filename" → raw bytes
    field_mappings: Dict[str, str]  # csv_column → snow_field
    file_col: str                   # CSV column: attachment filename  e.g. "u_file_name"
    file_path_col: str              # CSV column: subfolder/key        e.g. "u_folder_name"
    attach_field: str               # ServiceNow field for the attachment ref


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


def _find_file_bytes(
    files_map: Dict[str, bytes],
    folder_name: str,
    file_name: str,
) -> Optional[bytes]:
    """
    Locate file bytes using a two-pass strategy that handles both upload modes:

    Mode A — Folder upload (webkitdirectory):
        Browser sends full relative paths, so files_map keys are composite:
            "a/a.pdf", "b/a.pdf", "c/b.pdf"
        → Try composite key first.

    Mode B — Individual file selection:
        Browser sends bare filenames only, so files_map keys are flat:
            "a.pdf", "b.pdf"
        → Composite key misses; fall back to bare filename, but ONLY when
          that filename is unambiguous (exactly one key in files_map ends
          with the bare filename).  If two different folders both uploaded
          a file called "a.pdf" and they ended up as the same bare key,
          that is a collision and we refuse to guess.
    """
    # 1. Composite key: "a/a.pdf" — correct for folder uploads
    if folder_name:
        composite = f"{folder_name}/{file_name}"
        if composite in files_map:
            return files_map[composite]

    # 2. Bare key: "a.pdf" — for individual file selection
    #    Safe only when the bare filename is unambiguous in files_map.
    #    Count how many keys match (handles both flat keys and nested keys
    #    that end with the same filename segment).
    matches = [k for k in files_map if k == file_name or k.endswith(f"/{file_name}")]
    if len(matches) == 1:
        return files_map[matches[0]]

    # len == 0 → not found at all
    # len >= 2 → ambiguous; two files share the same bare name, refuse to guess
    return None


def process_row(row: dict, index: int, ctx: UploadContext) -> None:
    """
    Process a single CSV row: create record → upload attachment → link → hide banner.

    The attachment is always stored in ServiceNow as "folder/file.pdf"
    (e.g. "a/a.pdf") regardless of whether the local files were nested or flat.
    This keeps the ServiceNow File Name column consistent.
    """
    file_name   = row.get(ctx.file_col,      "").strip()   # e.g. "b.pdf"
    folder_name = row.get(ctx.file_path_col, "").strip()   # e.g. "c"

    # The filename that will be stored in ServiceNow's File Name column
    sn_file_name = f"{folder_name}/{file_name}" if folder_name else file_name

    file_bytes = _find_file_bytes(ctx.files_map, folder_name, file_name)

    if file_bytes is None:
        composite = f"{folder_name}/{file_name}" if folder_name else file_name
        # Distinguish total miss vs ambiguous bare-name collision
        matches = [k for k in ctx.files_map if k == file_name or k.endswith(f"/{file_name}")]
        if len(matches) >= 2:
            reason = (
                f"ambiguous — {len(matches)} uploaded files share the name '{file_name}' "
                f"({', '.join(matches)}). Upload via folder selection to resolve."
            )
        else:
            reason = (
                f"not found. Tried composite '{composite}' and bare '{file_name}'. "
                f"Available keys: {sorted(ctx.files_map.keys())}"
            )
        store.record_outcome(ctx.job_id, "skipped", f"SKIP [{index}] {reason}")
        return

    # Build the record payload, excluding the attachment reference field itself
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

            record_sys_id = _create_record(session, base, ctx.table_name, record_payload)

            attachment_sys_id = _upload_attachment(
                session, base, ctx.table_name, record_sys_id,
                sn_file_name,   # stored as "c/b.pdf" in ServiceNow File Name column
                file_bytes,
            )

            _link_attachment(
                session, base, ctx.table_name, record_sys_id,
                ctx.attach_field, attachment_sys_id,
            )
            _hide_attachment_from_banner(session, base, attachment_sys_id, ctx.table_name)

            store.record_outcome(
                ctx.job_id, "success",
                f"OK  [{index}] '{sn_file_name}'",
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
        f"FAIL [{index}] '{sn_file_name}' — {last_exc}",
    )