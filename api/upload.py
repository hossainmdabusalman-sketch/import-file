"""
POST /api/upload

Handles multipart form parsing, validation, job creation, and spawns the
background worker pool.  All heavy lifting is delegated to ``services/``.
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


def _parse_field_mappings(raw: str) -> dict[str, str]:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def _guess_file_col(headers: list[str]) -> str:
    for h in headers:
        if h.lower() in DEFAULT_FILE_COL_GUESSES:
            return h
    return headers[0]


def _parse_csv(content: bytes) -> list[dict]:
    text = content.decode("utf-8")
    rows = list(csv.DictReader(io.StringIO(text)))
    return rows


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


@router.post("/upload")
async def upload(request: Request) -> dict:
    try:
        form = await request.form(max_files=MAX_FORM_FILES)

        instance   = form.get("instance",   "").strip()
        username   = form.get("username",   "").strip()
        password   = form.get("password",   "").strip()
        table_name = form.get("table_name", "").strip()

        missing = [k for k, v in dict(instance=instance, username=username,
                                      password=password, table_name=table_name).items() if not v]
        if missing:
            raise HTTPException(400, f"Missing required fields: {', '.join(missing)}")

        max_workers    = int(form.get("max_workers", DEFAULT_MAX_WORKERS) or DEFAULT_MAX_WORKERS)
        field_mappings = _parse_field_mappings(form.get("field_mappings", "{}"))
        file_col       = form.get("file_col", "").strip()
        attach_field   = form.get("attach_field", DEFAULT_ATTACH_FIELD).strip() or DEFAULT_ATTACH_FIELD
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
        uploaded_files = form.getlist("files")
        if not uploaded_files:
            raise HTTPException(400, "At least one attachment file is required")

        files_map: dict[str, bytes] = {}
        for f in uploaded_files:
            files_map[f.filename] = await f.read()

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