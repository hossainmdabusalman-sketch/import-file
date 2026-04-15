"""
Thread-safe, in-memory registry of upload jobs.

All public functions acquire the lock internally so callers never have to
think about synchronisation.
"""
from __future__ import annotations

import threading
from typing import Dict, Optional

from models.job import Job, JobStatus


class JobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()

    def create(self, job_id: str, total: int) -> Job:
        job = Job(id=job_id, total=total)
        with self._lock:
            self._jobs[job_id] = job
        return job

    def record_outcome(
        self,
        job_id: str,
        outcome: str,          # "success" | "failed" | "skipped"
        message: str,
        bytes_uploaded: int = 0,
    ) -> None:
        with self._lock:
            job = self._jobs[job_id]
            setattr(job, outcome, getattr(job, outcome) + 1)
            job.logs.append(message)
            if bytes_uploaded:
                job.data_mb += bytes_uploaded / 1_048_576
            job.recalculate_throughput()
            if job.processed >= job.total:
                job.mark_done()

    def finish(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job and job.status != JobStatus.DONE:
                job.mark_done()

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def get_dict(self, job_id: str) -> Optional[dict]:
        job = self.get(job_id)
        return job.to_dict() if job else None


store = JobStore()