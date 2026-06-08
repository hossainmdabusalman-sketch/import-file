# """
# POST /api/upload

# Handles multipart form parsing, validation, job creation, and spawns the
# background worker pool.  All heavy lifting is delegated to ``services/``.
# """
# from __future__ import annotations

# import csv
# import io
# import json
# import os
# import threading
# import uuid
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from fastapi import APIRouter, HTTPException, Request

# from config import (
#     DEFAULT_ATTACH_FIELD,
#     DEFAULT_FILE_COL_GUESSES,
#     DEFAULT_MAX_WORKERS,
#     MAX_FORM_FILES,
# )
# from services.job_store import store
# from services.uploader import UploadContext, process_row

# router = APIRouter(prefix="/api", tags=["upload"])


# def _parse_field_mappings(raw: str) -> dict[str, str]:
#     try:
#         return json.loads(raw)
#     except (json.JSONDecodeError, TypeError):
#         return {}


# def _guess_file_col(headers: list[str]) -> str:
#     for h in headers:
#         if h.lower() in DEFAULT_FILE_COL_GUESSES:
#             return h
#     return headers[0]


# def _parse_csv(content: bytes) -> list[dict]:
#     text = content.decode("utf-8")
#     rows = list(csv.DictReader(io.StringIO(text)))
#     return rows


# def _run_job(rows: list[dict], ctx: UploadContext, max_workers: int) -> None:
#     with ThreadPoolExecutor(max_workers=max_workers) as pool:
#         futures = [
#             pool.submit(process_row, row, i + 1, ctx)
#             for i, row in enumerate(rows)
#         ]
#         for fut in as_completed(futures):
#             try:
#                 fut.result()
#             except Exception as exc:
#                 store.record_outcome(ctx.job_id, "failed", f"UNHANDLED: {exc}")

#     store.finish(ctx.job_id)


# @router.post("/upload")
# async def upload(request: Request) -> dict:
#     try:
#         form = await request.form(max_files=MAX_FORM_FILES)

#         instance   = form.get("instance",   "").strip()
#         username   = form.get("username",   "").strip()
#         password   = form.get("password",   "").strip()
#         table_name = form.get("table_name", "").strip()

#         missing = [k for k, v in dict(instance=instance, username=username,
#                                       password=password, table_name=table_name).items() if not v]
#         if missing:
#             raise HTTPException(400, f"Missing required fields: {', '.join(missing)}")

#         max_workers    = int(form.get("max_workers", DEFAULT_MAX_WORKERS) or DEFAULT_MAX_WORKERS)
#         field_mappings = _parse_field_mappings(form.get("field_mappings", "{}"))
#         file_col       = form.get("file_col", "").strip()
#         attach_field   = form.get("attach_field", DEFAULT_ATTACH_FIELD).strip() or DEFAULT_ATTACH_FIELD
#         csv_file = form.get("csv_file")
#         if not csv_file:
#             raise HTTPException(400, "CSV file is required")

#         rows = _parse_csv(await csv_file.read())
#         if not rows:
#             raise HTTPException(400, "CSV file is empty or has no data rows")

#         headers = list(rows[0].keys())

#         if not field_mappings:
#             field_mappings = {col: col for col in headers}
#         if not file_col:
#             file_col = _guess_file_col(headers)
#         uploaded_files = form.getlist("files")
#         if not uploaded_files:
#             raise HTTPException(400, "At least one attachment file is required")

#         files_map: dict[str, bytes] = {}
#         for f in uploaded_files:
#             bare_name = os.path.basename(f.filename)  # strip subfolder path
#             files_map[bare_name] = await f.read()

#         job_id = str(uuid.uuid4())[:8]
#         store.create(job_id, total=len(rows))

#         ctx = UploadContext(
#             job_id=job_id,
#             instance=instance,
#             username=username,
#             password=password,
#             table_name=table_name,
#             files_map=files_map,
#             field_mappings=field_mappings,
#             file_col=file_col,
#             attach_field=attach_field,
#         )

#         threading.Thread(
#             target=_run_job,
#             args=(rows, ctx, max_workers),
#             daemon=True,
#             name=f"upload-job-{job_id}",
#         ).start()

#         return {"job_id": job_id, "total": len(rows)}

#     except HTTPException:
#         raise
#     except Exception as exc:
#         import traceback
#         traceback.print_exc()
#         raise HTTPException(500, f"Unexpected server error: {exc}")


# """
# POST /api/upload

# Handles multipart form parsing, validation, job creation, and spawns the
# background worker pool.  All heavy lifting is delegated to ``services/``.
# """
# from __future__ import annotations

# import csv
# import io
# import json
# import os
# import threading
# import uuid
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from fastapi import APIRouter, HTTPException, Request

# from config import (
#     DEFAULT_ATTACH_FIELD,
#     DEFAULT_FILE_COL_GUESSES,
#     DEFAULT_MAX_WORKERS,
#     MAX_FORM_FILES,
# )
# from services.job_store import store
# from services.uploader import UploadContext, process_row

# router = APIRouter(prefix="/api", tags=["upload"])


# def _parse_field_mappings(raw: str) -> dict[str, str]:
#     try:
#         return json.loads(raw)
#     except (json.JSONDecodeError, TypeError):
#         return {}


# def _guess_file_col(headers: list[str]) -> str:
#     for h in headers:
#         if h.lower() in DEFAULT_FILE_COL_GUESSES:
#             return h
#     return headers[-1] if headers else ""


# def _guess_file_path_col(headers: list[str]) -> str:
#     """Guess the folder/key column — defaults to the first column."""
#     path_guesses = ["logical_key", "folder", "folder_name", "key", "col1_logical_key"]
#     for h in headers:
#         if h.lower() in path_guesses:
#             return h
#     return headers[0] if headers else ""


# def _parse_csv(content: bytes) -> list[dict]:
#     text = content.decode("utf-8")
#     rows = list(csv.DictReader(io.StringIO(text)))
#     return rows


# def _run_job(rows: list[dict], ctx: UploadContext, max_workers: int) -> None:
#     with ThreadPoolExecutor(max_workers=max_workers) as pool:
#         futures = [
#             pool.submit(process_row, row, i + 1, ctx)
#             for i, row in enumerate(rows)
#         ]
#         for fut in as_completed(futures):
#             try:
#                 fut.result()
#             except Exception as exc:
#                 store.record_outcome(ctx.job_id, "failed", f"UNHANDLED: {exc}")

#     store.finish(ctx.job_id)


# @router.post("/upload")
# async def upload(request: Request) -> dict:
#     try:
#         form = await request.form(max_files=MAX_FORM_FILES)

#         instance   = form.get("instance",   "").strip()
#         username   = form.get("username",   "").strip()
#         password   = form.get("password",   "").strip()
#         table_name = form.get("table_name", "").strip()

#         missing = [k for k, v in dict(instance=instance, username=username,
#                                       password=password, table_name=table_name).items() if not v]
#         if missing:
#             raise HTTPException(400, f"Missing required fields: {', '.join(missing)}")

#         max_workers    = int(form.get("max_workers", DEFAULT_MAX_WORKERS) or DEFAULT_MAX_WORKERS)
#         field_mappings = _parse_field_mappings(form.get("field_mappings", "{}"))
#         file_col       = form.get("file_col", "").strip()
#         file_path_col  = form.get("file_path_col", "").strip()   # folder/key column
#         attach_field   = form.get("attach_field", DEFAULT_ATTACH_FIELD).strip() or DEFAULT_ATTACH_FIELD

#         csv_file = form.get("csv_file")
#         if not csv_file:
#             raise HTTPException(400, "CSV file is required")

#         rows = _parse_csv(await csv_file.read())
#         if not rows:
#             raise HTTPException(400, "CSV file is empty or has no data rows")

#         headers = list(rows[0].keys())

#         if not field_mappings:
#             field_mappings = {col: col for col in headers}
#         if not file_col:
#             file_col = _guess_file_col(headers)
#         if not file_path_col:
#             file_path_col = _guess_file_path_col(headers)

#         uploaded_files = form.getlist("files")
#         if not uploaded_files:
#             raise HTTPException(400, "At least one attachment file is required")

#         # Build files_map keyed by "subfolder/filename" (e.g. "c/b.pdf")
#         # The browser sends webkitRelativePath as the filename:
#         #   "master-test_copy/c/b.pdf"
#         # We strip the first segment (the root folder) to get "c/b.pdf"
#         files_map: dict[str, bytes] = {}
#         for f in uploaded_files:
#             raw_path = f.filename.replace("\\", "/")   # normalise Windows paths
#             parts    = raw_path.split("/")

#             if len(parts) > 1:
#                 # Drop the root/master folder segment → "c/b.pdf"
#                 relative_key = "/".join(parts[1:])
#             else:
#                 # Flat file with no subfolder — use bare filename
#                 relative_key = parts[0]

#             files_map[relative_key] = await f.read()

#         job_id = str(uuid.uuid4())[:8]
#         store.create(job_id, total=len(rows))

#         ctx = UploadContext(
#             job_id=job_id,
#             instance=instance,
#             username=username,
#             password=password,
#             table_name=table_name,
#             files_map=files_map,
#             field_mappings=field_mappings,
#             file_col=file_col,
#             file_path_col=file_path_col,   # e.g. "col1_logical_key"
#             attach_field=attach_field,
#         )

#         threading.Thread(
#             target=_run_job,
#             args=(rows, ctx, max_workers),
#             daemon=True,
#             name=f"upload-job-{job_id}",
#         ).start()

#         return {"job_id": job_id, "total": len(rows)}

#     except HTTPException:
#         raise
#     except Exception as exc:
#         import traceback
#         traceback.print_exc()
#         raise HTTPException(500, f"Unexpected server error: {exc}")

# """
# POST /api/upload

# Handles multipart form parsing, validation, job creation, and spawns the
# background worker pool.  All heavy lifting is delegated to ``services/``.
# """
# from __future__ import annotations

# import csv
# import io
# import json
# import os
# import threading
# import uuid
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from fastapi import APIRouter, HTTPException, Request

# from config import (
#     DEFAULT_ATTACH_FIELD,
#     DEFAULT_FILE_COL_GUESSES,
#     DEFAULT_MAX_WORKERS,
#     MAX_FORM_FILES,
# )
# from services.job_store import store
# from services.uploader import UploadContext, process_row

# router = APIRouter(prefix="/api", tags=["upload"])


# def _parse_field_mappings(raw: str) -> dict[str, str]:
#     try:
#         return json.loads(raw)
#     except (json.JSONDecodeError, TypeError):
#         return {}


# def _guess_file_col(headers: list[str]) -> str:
#     for h in headers:
#         if h.lower() in DEFAULT_FILE_COL_GUESSES:
#             return h
#     return headers[-1] if headers else ""


# def _guess_file_path_col(headers: list[str]) -> str:
#     path_guesses = ["u_folder_name", "logical_key", "folder", "folder_name", "key", "col1_logical_key"]
#     for h in headers:
#         if h.lower() in path_guesses:
#             return h
#     return headers[0] if headers else ""


# def _parse_csv(content: bytes) -> list[dict]:
#     text = content.decode("utf-8")
#     rows = list(csv.DictReader(io.StringIO(text)))
#     return rows


# def _run_job(rows: list[dict], ctx: UploadContext, max_workers: int) -> None:
#     with ThreadPoolExecutor(max_workers=max_workers) as pool:
#         futures = [
#             pool.submit(process_row, row, i + 1, ctx)
#             for i, row in enumerate(rows)
#         ]
#         for fut in as_completed(futures):
#             try:
#                 fut.result()
#             except Exception as exc:
#                 store.record_outcome(ctx.job_id, "failed", f"UNHANDLED: {exc}")

#     store.finish(ctx.job_id)


# @router.post("/upload")
# async def upload(request: Request) -> dict:
#     try:
#         form = await request.form(max_files=MAX_FORM_FILES)

#         instance   = form.get("instance",   "").strip()
#         username   = form.get("username",   "").strip()
#         password   = form.get("password",   "").strip()
#         table_name = form.get("table_name", "").strip()

#         missing = [k for k, v in dict(instance=instance, username=username,
#                                       password=password, table_name=table_name).items() if not v]
#         if missing:
#             raise HTTPException(400, f"Missing required fields: {', '.join(missing)}")

#         max_workers    = int(form.get("max_workers", DEFAULT_MAX_WORKERS) or DEFAULT_MAX_WORKERS)
#         field_mappings = _parse_field_mappings(form.get("field_mappings", "{}"))
#         file_col       = form.get("file_col", "").strip()
#         file_path_col  = form.get("file_path_col", "").strip()
#         attach_field   = form.get("attach_field", DEFAULT_ATTACH_FIELD).strip() or DEFAULT_ATTACH_FIELD

#         csv_file = form.get("csv_file")
#         if not csv_file:
#             raise HTTPException(400, "CSV file is required")

#         rows = _parse_csv(await csv_file.read())
#         if not rows:
#             raise HTTPException(400, "CSV file is empty or has no data rows")

#         headers = list(rows[0].keys())

#         if not field_mappings:
#             field_mappings = {col: col for col in headers}
#         if not file_col:
#             file_col = _guess_file_col(headers)
#         if not file_path_col:
#             file_path_col = _guess_file_path_col(headers)

#         uploaded_files = form.getlist("files")
#         if not uploaded_files:
#             raise HTTPException(400, "At least one attachment file is required")

#         # ── Build files_map ───────────────────────────────────────────────────
#         # The browser sends each file's webkitRelativePath as its filename.
#         # e.g.  "master-test_copy/a/a.pdf"  →  strip root  →  key = "a/a.pdf"
#         # e.g.  "master-test_copy/a.pdf"    →  strip root  →  key = "a.pdf"  (flat)
#         files_map: dict[str, bytes] = {}
#         print("\n=== FILES RECEIVED FROM BROWSER ===")
#         for f in uploaded_files:
#             raw_path = f.filename.replace("\\", "/")
#             parts    = [p for p in raw_path.split("/") if p]   # drop empty segments

#             if len(parts) > 1:
#                 # Strip only the first segment (the root/master folder)
#                 relative_key = "/".join(parts[1:])
#             else:
#                 relative_key = parts[0] if parts else f.filename

#             print(f"  raw='{raw_path}'  →  key='{relative_key}'")
#             files_map[relative_key] = await f.read()

#         print(f"\nfiles_map keys: {list(files_map.keys())}")
#         print(f"file_col='{file_col}'  file_path_col='{file_path_col}'")
#         print(f"CSV rows sample: {rows[:2]}")
#         print("===================================\n")

#         job_id = str(uuid.uuid4())[:8]
#         store.create(job_id, total=len(rows))

#         ctx = UploadContext(
#             job_id=job_id,
#             instance=instance,
#             username=username,
#             password=password,
#             table_name=table_name,
#             files_map=files_map,
#             field_mappings=field_mappings,
#             file_col=file_col,
#             file_path_col=file_path_col,
#             attach_field=attach_field,
#         )

#         threading.Thread(
#             target=_run_job,
#             args=(rows, ctx, max_workers),
#             daemon=True,
#             name=f"upload-job-{job_id}",
#         ).start()

#         return {"job_id": job_id, "total": len(rows)}

#     except HTTPException:
#         raise
#     except Exception as exc:
#         import traceback
#         traceback.print_exc()
#         raise HTTPException(500, f"Unexpected server error: {exc}")


"""
POST /api/upload

Handles multipart form parsing, validation, job creation, and spawns the
background worker pool.  All heavy lifting is delegated to ``services/``.

files_map key convention
------------------------
The browser sends each file's webkitRelativePath as its filename field:

  "masterfolder/a/a.pdf"   →  strip root segment  →  key = "a/a.pdf"
  "masterfolder/b/b.pdf"   →  strip root segment  →  key = "b/b.pdf"
  "masterfolder/c/b.pdf"   →  strip root segment  →  key = "c/b.pdf"   ← unique!
  "masterfolder/a.pdf"     →  strip root segment  →  key = "a.pdf"     (flat)

This means two files with the same bare filename in different subfolders are
always stored under distinct keys and can never be confused.
"""
from __future__ import annotations

import csv
import io
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import APIRouter, HTTPException, Request

from config import (
    DEFAULT_ATTACH_FIELD,
    DEFAULT_FILE_COL_GUESSES,
    DEFAULT_MAX_WORKERS,
    MAX_FORM_FILES,
)
from services.job_store import store
from services.uploader import UploadContext, process_row

router = APIRouter(prefix="/api", tags=["upload"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_field_mappings(raw: str) -> dict[str, str]:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def _guess_file_col(headers: list[str]) -> str:
    """Return the first header that looks like a filename column."""
    for h in headers:
        if h.lower() in DEFAULT_FILE_COL_GUESSES:
            return h
    return headers[-1] if headers else ""


def _guess_file_path_col(headers: list[str]) -> str:
    """Return the first header that looks like a folder/path column."""
    path_guesses = {
        "u_folder_name", "folder_name", "folder",
        "logical_key", "col1_logical_key", "key",
    }
    for h in headers:
        if h.lower() in path_guesses:
            return h
    return headers[0] if headers else ""


def _parse_csv(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig")   # strip BOM if present
    return list(csv.DictReader(io.StringIO(text)))


def _relative_key(raw_path: str) -> str:
    """
    Convert a browser webkitRelativePath to a files_map key by stripping the
    root/master folder segment.

    "masterfolder/c/b.pdf"  →  "c/b.pdf"
    "masterfolder/b.pdf"    →  "b.pdf"
    "b.pdf"                 →  "b.pdf"   (no subfolder sent by browser)
    """
    normalized = raw_path.replace("\\", "/")
    parts = [p for p in normalized.split("/") if p]   # drop empty segments
    if len(parts) > 1:
        return "/".join(parts[1:])   # drop the root folder
    return parts[0] if parts else raw_path


def _run_job(rows: list[dict], ctx: UploadContext, max_workers: int) -> None:
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(process_row, row, i + 1, ctx)
            for i, row in enumerate(rows)
        ]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                store.record_outcome(ctx.job_id, "failed", f"UNHANDLED: {exc}")

    store.finish(ctx.job_id)


# ── Route ─────────────────────────────────────────────────────────────────────

@router.post("/upload")
async def upload(request: Request) -> dict:
    try:
        form = await request.form(max_files=MAX_FORM_FILES)

        # ── Required credentials ──────────────────────────────────────────────
        instance   = form.get("instance",   "").strip()
        username   = form.get("username",   "").strip()
        password   = form.get("password",   "").strip()
        table_name = form.get("table_name", "").strip()

        missing = [
            k for k, v in {
                "instance":   instance,
                "username":   username,
                "password":   password,
                "table_name": table_name,
            }.items() if not v
        ]
        if missing:
            raise HTTPException(400, f"Missing required fields: {', '.join(missing)}")

        # ── Optional config ───────────────────────────────────────────────────
        max_workers    = int(form.get("max_workers", DEFAULT_MAX_WORKERS) or DEFAULT_MAX_WORKERS)
        field_mappings = _parse_field_mappings(form.get("field_mappings", "{}"))
        file_col       = form.get("file_col",      "").strip()
        file_path_col  = form.get("file_path_col", "").strip()
        attach_field   = (
            form.get("attach_field", DEFAULT_ATTACH_FIELD).strip()
            or DEFAULT_ATTACH_FIELD
        )

        # ── CSV ───────────────────────────────────────────────────────────────
        csv_file = form.get("csv_file")
        if not csv_file:
            raise HTTPException(400, "CSV file is required")

        rows = _parse_csv(await csv_file.read())
        if not rows:
            raise HTTPException(400, "CSV file is empty or has no data rows")

        headers = list(rows[0].keys())

        if not field_mappings:
            field_mappings = {col: col for col in headers}
        if not file_col:
            file_col = _guess_file_col(headers)
        if not file_path_col:
            file_path_col = _guess_file_path_col(headers)

        # ── Uploaded files → files_map ────────────────────────────────────────
        # Key = relative path with root folder stripped: "c/b.pdf", "b/b.pdf"
        # Two files sharing the same bare name in different subfolders will
        # always produce distinct keys, so they can never be confused.
        uploaded_files = form.getlist("files")
        if not uploaded_files:
            raise HTTPException(400, "At least one attachment file is required")

        files_map: dict[str, bytes] = {}
        print("\n=== FILES RECEIVED FROM BROWSER ===")
        for f in uploaded_files:
            key = _relative_key(f.filename)
            print(f"  raw='{f.filename}'  →  key='{key}'")
            files_map[key] = await f.read()

        print(f"\nfiles_map keys : {sorted(files_map.keys())}")
        print(f"file_col       : '{file_col}'")
        print(f"file_path_col  : '{file_path_col}'")
        print(f"CSV rows sample: {rows[:3]}")
        print("===================================\n")

        # ── Create job and start background thread ────────────────────────────
        job_id = str(uuid.uuid4())[:8]
        store.create(job_id, total=len(rows))

        ctx = UploadContext(
            job_id=job_id,
            instance=instance,
            username=username,
            password=password,
            table_name=table_name,
            files_map=files_map,
            field_mappings=field_mappings,
            file_col=file_col,
            file_path_col=file_path_col,
            attach_field=attach_field,
        )

        threading.Thread(
            target=_run_job,
            args=(rows, ctx, max_workers),
            daemon=True,
            name=f"upload-job-{job_id}",
        ).start()

        return {"job_id": job_id, "total": len(rows)}

    except HTTPException:
        raise
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Unexpected server error: {exc}")