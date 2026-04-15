# import csv
# import io
# import json
# import os
# import time
# import threading
# import requests
# from requests.auth import HTTPBasicAuth
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from typing import Dict, List, Optional

# from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
# from fastapi.responses import HTMLResponse, JSONResponse
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.staticfiles import StaticFiles

# # ── In-memory job store ────────────────────────────────────────────────────────
# jobs: Dict[str, dict] = {}
# jobs_lock = threading.Lock()

# def new_job(job_id: str, total: int):
#     with jobs_lock:
#         jobs[job_id] = {
#             "id": job_id,
#             "status": "running",
#             "total": total,
#             "success": 0,
#             "failed": 0,
#             "skipped": 0,
#             "logs": [],
#             "start_time": time.time(),
#             "end_time": None,
#             "throughput": 0.0,
#             "data_mb": 0.0,
#         }

# def update_job(job_id: str, outcome: str, msg: str, bytes_: int = 0):
#     with jobs_lock:
#         j = jobs[job_id]
#         j[outcome] += 1
#         j["logs"].append(msg)
#         if bytes_:
#             j["data_mb"] += bytes_ / 1_048_576
#         done = j["success"] + j["failed"] + j["skipped"]
#         elapsed = time.time() - j["start_time"]
#         j["throughput"] = round(done / elapsed, 2) if elapsed > 0 else 0
#         if done >= j["total"]:
#             j["status"] = "done"
#             j["end_time"] = time.time()

# def finish_job(job_id: str):
#     with jobs_lock:
#         j = jobs[job_id]
#         j["status"] = "done"
#         j["end_time"] = time.time()

# # ── Thread-local session ───────────────────────────────────────────────────────
# _tls = threading.local()

# def get_session(username: str, password: str) -> requests.Session:
#     if not hasattr(_tls, "session"):
#         s = requests.Session()
#         s.auth = HTTPBasicAuth(username, password)
#         s.headers.update({"Accept": "application/json"})
#         adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
#         s.mount("https://", adapter)
#         s.mount("http://", adapter)
#         _tls.session = s
#     return _tls.session

# # ── Worker ─────────────────────────────────────────────────────────────────────
# def process_row(
#     row: dict,
#     index: int,
#     job_id: str,
#     instance: str,
#     username: str,
#     password: str,
#     table_name: str,
#     files_map: dict,
#     field_mappings: dict,   # {csv_col: snow_field_name}
#     file_col: str,          # which csv column holds the attachment filename
#     max_retries: int = 3,
# ):
#     session = get_session(username, password)

#     # Get the attachment filename from the designated file column
#     file_name = row.get(file_col, "").strip()

#     if file_name not in files_map:
#         update_job(job_id, "skipped", f"SKIP [{index}] {file_name} — not in uploaded folder")
#         return

#     file_bytes = files_map[file_name]

#     # Build the record payload dynamically using the field mappings
#     # csv column name → ServiceNow field name, value from row
#     record_payload = {}
#     for csv_col, snow_field in field_mappings.items():
#         if csv_col in row:
#             record_payload[snow_field] = row[csv_col].strip()

#     last_exc = None
#     for attempt in range(1, max_retries + 1):
#         try:
#             # Create record
#             url = f"{instance.rstrip('/')}/api/now/table/{table_name}"
#             resp = session.post(url, json=record_payload, timeout=30)
#             resp.raise_for_status()
#             sys_id = resp.json()["result"]["sys_id"]

#             # Attach file
#             att_url = (
#                 f"{instance.rstrip('/')}/api/now/attachment/file"
#                 f"?table_name={table_name}&table_sys_id={sys_id}&file_name={file_name}"
#             )
#             att_resp = session.post(
#                 att_url, data=file_bytes,
#                 headers={"Content-Type": "application/pdf"},
#                 timeout=60,
#             )
#             att_resp.raise_for_status()
#             update_job(job_id, "success", f"OK [{index}] {file_name}", len(file_bytes))
#             return

#         except requests.HTTPError as e:
#             last_exc = e
#             status = e.response.status_code if e.response is not None else 0
#             backoff = 0.5 * (3 if status == 429 else 1) * (2 ** (attempt - 1))
#             if attempt < max_retries:
#                 time.sleep(backoff)
#         except Exception as e:
#             last_exc = e
#             if attempt < max_retries:
#                 time.sleep(0.5 * attempt)

#     update_job(job_id, "failed", f"FAIL [{index}] {file_name} — {last_exc}")


# # ── FastAPI app ────────────────────────────────────────────────────────────────
# app = FastAPI(title="ServiceNow Bulk Uploader")
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
#     allow_headers=["*"],
#     allow_credentials=True,
# )

# @app.get("/api/health")
# async def health():
#     return {"status": "ok"}

# @app.post("/api/upload")
# async def upload(request: Request):
#     try:
#         form = await request.form(max_files=2500)

#         instance        = form.get("instance", "").strip()
#         username        = form.get("username", "").strip()
#         password        = form.get("password", "").strip()
#         table_name      = form.get("table_name", "").strip()
#         max_workers_str = form.get("max_workers", "20")
#         field_mappings_raw = form.get("field_mappings", "{}")
#         file_col        = form.get("file_col", "").strip()

#         # Validate required fields
#         if not all([instance, username, password, table_name]):
#             raise HTTPException(400, "Missing required fields: instance, username, password, table_name")

#         # Parse field mappings JSON
#         try:
#             field_mappings = json.loads(field_mappings_raw)
#         except Exception:
#             field_mappings = {}

#         csv_file = form.get("csv_file")
#         if not csv_file:
#             raise HTTPException(400, "CSV file is required")

#         files_list = form.getlist("files")
#         if not files_list:
#             raise HTTPException(400, "At least one attachment file is required")

#         try:
#             max_workers_int = int(max_workers_str)
#         except (ValueError, TypeError):
#             max_workers_int = 20

#         # Read CSV
#         csv_content = await csv_file.read()
#         rows = list(csv.DictReader(io.StringIO(csv_content.decode("utf-8"))))
#         if not rows:
#             raise HTTPException(400, "CSV file is empty")

#         # If no field_mappings provided, use CSV headers as-is
#         if not field_mappings:
#             field_mappings = {col: col for col in rows[0].keys()}

#         # If no file_col provided, default to first column that looks like a filename
#         if not file_col:
#             guesses = ['related_file', 'file_name', 'filename', 'attachment', 'file']
#             headers = list(rows[0].keys())
#             file_col = next((h for h in headers if h.lower() in guesses), headers[0])

#         # Build files map name → bytes
#         files_map = {}
#         for f in files_list:
#             content = await f.read()
#             files_map[f.filename] = content

#         # Create job
#         import uuid
#         job_id = str(uuid.uuid4())[:8]
#         new_job(job_id, len(rows))

#         # Submit work in background
#         def run():
#             with ThreadPoolExecutor(max_workers=max_workers_int) as pool:
#                 futs = [
#                     pool.submit(
#                         process_row,
#                         row, i + 1, job_id,
#                         instance, username, password,
#                         table_name, files_map,
#                         field_mappings, file_col,
#                     )
#                     for i, row in enumerate(rows)
#                 ]
#                 for fut in as_completed(futs):
#                     fut.result()
#             finish_job(job_id)

#         threading.Thread(target=run, daemon=True).start()
#         return {"job_id": job_id, "total": len(rows)}

#     except HTTPException:
#         raise
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         raise HTTPException(400, f"Upload error: {str(e)}")


# @app.get("/api/job/{job_id}")
# def job_status(job_id: str):
#     with jobs_lock:
#         j = jobs.get(job_id)
#     if not j:
#         raise HTTPException(404, "Job not found")
#     return j


# @app.get("/", response_class=HTMLResponse)
# def index():
#     with open("static/index.html", encoding="utf-8") as f:
#         return f.read()


# app.mount("/static", StaticFiles(directory="static"), name="static")


import csv
import io
import json
import os
import time
import threading
import requests
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# ── In-memory job store ────────────────────────────────────────────────────────
jobs: Dict[str, dict] = {}
jobs_lock = threading.Lock()

def new_job(job_id: str, total: int):
    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "status": "running",
            "total": total,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "logs": [],
            "start_time": time.time(),
            "end_time": None,
            "throughput": 0.0,
            "data_mb": 0.0,
        }

def update_job(job_id: str, outcome: str, msg: str, bytes_: int = 0):
    with jobs_lock:
        j = jobs[job_id]
        j[outcome] += 1
        j["logs"].append(msg)
        if bytes_:
            j["data_mb"] += bytes_ / 1_048_576
        done = j["success"] + j["failed"] + j["skipped"]
        elapsed = time.time() - j["start_time"]
        j["throughput"] = round(done / elapsed, 2) if elapsed > 0 else 0
        if done >= j["total"]:
            j["status"] = "done"
            j["end_time"] = time.time()

def finish_job(job_id: str):
    with jobs_lock:
        j = jobs[job_id]
        j["status"] = "done"
        j["end_time"] = time.time()

# ── Thread-local session ───────────────────────────────────────────────────────
_tls = threading.local()

def get_session(username: str, password: str) -> requests.Session:
    if not hasattr(_tls, "session"):
        s = requests.Session()
        s.auth = HTTPBasicAuth(username, password)
        s.headers.update({"Accept": "application/json"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _tls.session = s
    return _tls.session

# ── Worker ─────────────────────────────────────────────────────────────────────
def process_row(
    row: dict,
    index: int,
    job_id: str,
    instance: str,
    username: str,
    password: str,
    table_name: str,
    files_map: dict,
    field_mappings: dict,       # {csv_col: snow_field_name}
    file_col: str,              # which csv column holds the attachment filename
    attach_field: str = "file_attachment",  # the ServiceNow field name for the attachment reference
    max_retries: int = 3,
):
    session = get_session(username, password)

    # Get the attachment filename from the designated file column
    file_name = row.get(file_col, "").strip()

    if file_name not in files_map:
        update_job(job_id, "skipped", f"SKIP [{index}] {file_name} — not in uploaded folder")
        return

    file_bytes = files_map[file_name]

    # Build the record payload dynamically using the field mappings
    # Exclude the attach_field from payload — we'll set it after upload
    record_payload = {}
    for csv_col, snow_field in field_mappings.items():
        if csv_col in row and snow_field != attach_field:
            record_payload[snow_field] = row[csv_col].strip()

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            base = instance.rstrip('/')

            # ── Step 1: Create the record (without the attachment field yet) ──
            create_url = f"{base}/api/now/table/{table_name}"
            resp = session.post(create_url, json=record_payload, timeout=30)
            resp.raise_for_status()
            sys_id = resp.json()["result"]["sys_id"]

            # ── Step 2: Upload the file via the attachment API ──────────────
            att_url = (
                f"{base}/api/now/attachment/file"
                f"?table_name={table_name}&table_sys_id={sys_id}&file_name={file_name}"
            )
            # Detect content type by extension
            content_type = "application/pdf"
            lower = file_name.lower()
            if lower.endswith(".png"):
                content_type = "image/png"
            elif lower.endswith((".jpg", ".jpeg")):
                content_type = "image/jpeg"
            elif lower.endswith(".docx"):
                content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            elif lower.endswith(".xlsx"):
                content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif lower.endswith(".txt"):
                content_type = "text/plain"

            att_resp = session.post(
                att_url,
                data=file_bytes,
                headers={"Content-Type": content_type},
                timeout=60,
            )
            att_resp.raise_for_status()
            attachment_sys_id = att_resp.json()["result"]["sys_id"]

            # ── Step 3: PATCH the record to set the file_attachment field ───
            # ServiceNow attachment reference fields expect the sys_id of the
            # sys_attachment record (which is what we got above).
            patch_url = f"{base}/api/now/table/{table_name}/{sys_id}"
            patch_resp = session.patch(
                patch_url,
                json={attach_field: attachment_sys_id},
                timeout=30,
            )
            patch_resp.raise_for_status()

            update_job(job_id, "success", f"OK [{index}] {file_name}", len(file_bytes))
            return

        except requests.HTTPError as e:
            last_exc = e
            status = e.response.status_code if e.response is not None else 0
            backoff = 0.5 * (3 if status == 429 else 1) * (2 ** (attempt - 1))
            if attempt < max_retries:
                time.sleep(backoff)
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(0.5 * attempt)

    update_job(job_id, "failed", f"FAIL [{index}] {file_name} — {last_exc}")


# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(title="ServiceNow Bulk Uploader")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=True,
)

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.post("/api/upload")
async def upload(request: Request):
    try:
        form = await request.form(max_files=2500)

        instance           = form.get("instance", "").strip()
        username           = form.get("username", "").strip()
        password           = form.get("password", "").strip()
        table_name         = form.get("table_name", "").strip()
        max_workers_str    = form.get("max_workers", "20")
        field_mappings_raw = form.get("field_mappings", "{}")
        file_col           = form.get("file_col", "").strip()
        attach_field       = form.get("attach_field", "file_attachment").strip()  # NEW

        # Validate required fields
        if not all([instance, username, password, table_name]):
            raise HTTPException(400, "Missing required fields: instance, username, password, table_name")

        # Parse field mappings JSON
        try:
            field_mappings = json.loads(field_mappings_raw)
        except Exception:
            field_mappings = {}

        csv_file = form.get("csv_file")
        if not csv_file:
            raise HTTPException(400, "CSV file is required")

        files_list = form.getlist("files")
        if not files_list:
            raise HTTPException(400, "At least one attachment file is required")

        try:
            max_workers_int = int(max_workers_str)
        except (ValueError, TypeError):
            max_workers_int = 20

        # Read CSV
        csv_content = await csv_file.read()
        rows = list(csv.DictReader(io.StringIO(csv_content.decode("utf-8"))))
        if not rows:
            raise HTTPException(400, "CSV file is empty")

        # If no field_mappings provided, use CSV headers as-is
        if not field_mappings:
            field_mappings = {col: col for col in rows[0].keys()}

        # If no file_col provided, default to first column that looks like a filename
        if not file_col:
            guesses = ['related_file', 'file_name', 'filename', 'attachment', 'file']
            headers = list(rows[0].keys())
            file_col = next((h for h in headers if h.lower() in guesses), headers[0])

        # Build files map: name → bytes
        files_map = {}
        for f in files_list:
            content = await f.read()
            files_map[f.filename] = content

        # Create job
        import uuid
        job_id = str(uuid.uuid4())[:8]
        new_job(job_id, len(rows))

        # Submit work in background
        def run():
            with ThreadPoolExecutor(max_workers=max_workers_int) as pool:
                futs = [
                    pool.submit(
                        process_row,
                        row, i + 1, job_id,
                        instance, username, password,
                        table_name, files_map,
                        field_mappings, file_col,
                        attach_field,           # pass the configurable field name
                    )
                    for i, row in enumerate(rows)
                ]
                for fut in as_completed(futs):
                    fut.result()
            finish_job(job_id)

        threading.Thread(target=run, daemon=True).start()
        return {"job_id": job_id, "total": len(rows)}

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(400, f"Upload error: {str(e)}")


@app.get("/api/job/{job_id}")
def job_status(job_id: str):
    with jobs_lock:
        j = jobs.get(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    return j


@app.get("/", response_class=HTMLResponse)
def index():
    # with open("static/index.html", encoding="utf-8") as f:
    #     return f.read()
        # Works both locally and on Render
    base = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base, "static", "index.html"), encoding="utf-8") as f:
        return f.read()


app.mount("/static", StaticFiles(directory="static"), name="static")