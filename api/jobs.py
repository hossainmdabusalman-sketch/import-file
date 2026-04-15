"""
GET /api/job/{job_id}
"""
from fastapi import APIRouter, HTTPException

from services.job_store import store

router = APIRouter(prefix="/api", tags=["jobs"])


@router.get("/job/{job_id}")
def job_status(job_id: str) -> dict:
    data = store.get_dict(job_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return data