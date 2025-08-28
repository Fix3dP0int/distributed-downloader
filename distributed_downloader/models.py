"""Data models for the distributed downloader."""

from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime


class TaskStatus(str, Enum):
    """Task status enumeration."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class DownloadTask(BaseModel):
    """Download task model."""
    task_id: str
    dataset_name: str
    file_path: str
    file_url: str
    file_size: Optional[int] = None
    attempt_count: int = 0
    max_retries: int = 3
    status: TaskStatus = TaskStatus.PENDING
    worker_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = {}


class WorkerStatus(BaseModel):
    """Worker status model."""
    worker_id: str
    is_active: bool
    current_task: Optional[str] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    last_heartbeat: datetime
    metadata: Dict[str, Any] = {}


class JobStatus(BaseModel):
    """Overall job status model."""
    job_id: str
    dataset_name: str
    total_files: int
    completed_files: int = 0
    failed_files: int = 0
    active_workers: int = 0
    created_at: datetime
    updated_at: datetime
    status: TaskStatus = TaskStatus.PENDING