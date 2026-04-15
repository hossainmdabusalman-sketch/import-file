"""
Data model for an upload job.

Using a plain dataclass (instead of Pydantic) keeps things lightweight and
lets us mutate fields in-place inside the thread-safe job store.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List


class JobStatus(str, Enum):
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"


@dataclass
class Job:
    id: str
    total: int
    status: JobStatus = JobStatus.RUNNING

    success: int = 0
    failed: int = 0
    skipped: int = 0

    logs: List[str] = field(default_factory=list)

    start_time: float = field(default_factory=time.time)
    end_time: float | None = None

    throughput: float = 0.0  
    data_mb: float = 0.0      

    @property
    def processed(self) -> int:
        return self.success + self.failed + self.skipped

    @property
    def elapsed(self) -> float:
        end = self.end_time or time.time()
        return end - self.start_time

    def recalculate_throughput(self) -> None:
        if self.elapsed > 0:
            self.throughput = round(self.processed / self.elapsed, 2)

    def mark_done(self) -> None:
        self.status = JobStatus.DONE
        self.end_time = time.time()
        self.recalculate_throughput()

    def to_dict(self) -> dict:
        """Serialise to a plain dict for JSON responses."""
        return {
            "id": self.id,
            "status": self.status.value,
            "total": self.total,
            "success": self.success,
            "failed": self.failed,
            "skipped": self.skipped,
            "processed": self.processed,
            "logs": self.logs,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "elapsed_seconds": round(self.elapsed, 2),
            "throughput": self.throughput,
            "data_mb": round(self.data_mb, 4),
        }