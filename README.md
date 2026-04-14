# ServiceNow Bulk Uploader — Web UI

A FastAPI backend + HTML frontend for bulk-inserting CSV records and attaching files to ServiceNow.

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the server
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Usage

Open `http://localhost:8000` in your browser.

1. Fill in your ServiceNow **instance URL**, **username**, and **password**
2. Set your **table name** and **max workers** (threads)
3. Drop or browse your **CSV file** (columns: `rec_number`, `record_name`, `related_file`)
4. Drop or browse your **attachment files** (PDFs or any files referenced in the CSV)
5. Click **Start bulk upload →**

## CSV format

```csv
rec_number,record_name,related_file
REC001,My Record,document1.pdf
REC002,Another Record,document2.pdf
```

## REST API

### POST /api/upload
Multipart form fields:
- `instance` — ServiceNow instance URL
- `username` / `password`
- `table_name`
- `max_workers` — parallel threads (default 20)
- `csv_file` — CSV file upload
- `files` — one or more attachment files

Returns: `{ "job_id": "abc12345", "total": 2000 }`

### GET /api/job/{job_id}
Poll job status. Returns:
```json
{
  "id": "abc12345",
  "status": "running|done",
  "total": 2000,
  "success": 1523,
  "failed": 3,
  "skipped": 0,
  "throughput": 18.4,
  "data_mb": 45.2,
  "logs": ["OK [1] file1.pdf", ...]
}
```
