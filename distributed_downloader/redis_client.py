"""Redis client for message queue operations."""

import json
import redis
from typing import Optional, List, Dict, Any
from loguru import logger

from .models import DownloadTask, TaskStatus, WorkerStatus, JobStatus


class RedisClient:
    """Redis client for managing download tasks and worker coordination."""
    
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, password: Optional[str] = None):
        """Initialize Redis client."""
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        
        # Queue names
        self.task_queue = "downloader:tasks"
        self.failed_queue = "downloader:failed_tasks"
        self.workers_key = "downloader:workers"
        self.jobs_key = "downloader:jobs"
        
    def ping(self) -> bool:
        """Check Redis connection."""
        try:
            return self.redis_client.ping()
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            return False
    
    def enqueue_task(self, task: DownloadTask) -> bool:
        """Add task to the download queue."""
        try:
            task_data = task.model_dump_json()
            self.redis_client.lpush(self.task_queue, task_data)
            logger.info(f"Enqueued task {task.task_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to enqueue task {task.task_id}: {e}")
            return False
    
    def dequeue_task(self, timeout: int = 10) -> Optional[DownloadTask]:
        """Get next task from the download queue."""
        try:
            result = self.redis_client.brpop(self.task_queue, timeout=timeout)
            if result:
                _, task_data = result
                task = DownloadTask.model_validate_json(task_data)
                logger.info(f"Dequeued task {task.task_id}")
                return task
            return None
        except Exception as e:
            logger.error(f"Failed to dequeue task: {e}")
            return None
    
    def update_task_status(self, task_id: str, status: TaskStatus, 
                          worker_id: Optional[str] = None,
                          error_message: Optional[str] = None) -> bool:
        """Update task status."""
        try:
            key = f"downloader:task:{task_id}"
            updates = {
                "status": status.value,
                "updated_at": json.dumps({"timestamp": "now"}),
            }
            if worker_id:
                updates["worker_id"] = worker_id
            if error_message:
                updates["error_message"] = error_message
                
            self.redis_client.hmset(key, updates)
            logger.info(f"Updated task {task_id} status to {status}")
            return True
        except Exception as e:
            logger.error(f"Failed to update task {task_id} status: {e}")
            return False
    
    def requeue_failed_task(self, task: DownloadTask) -> bool:
        """Requeue a failed task for retry."""
        try:
            task.attempt_count += 1
            task.status = TaskStatus.RETRYING
            
            if task.attempt_count <= task.max_retries:
                return self.enqueue_task(task)
            else:
                # Move to failed queue
                task_data = task.model_dump_json()
                self.redis_client.lpush(self.failed_queue, task_data)
                logger.warning(f"Task {task.task_id} exceeded max retries, moved to failed queue")
                return False
        except Exception as e:
            logger.error(f"Failed to requeue task {task.task_id}: {e}")
            return False
    
    def register_worker(self, worker_status: WorkerStatus) -> bool:
        """Register a worker."""
        try:
            key = f"{self.workers_key}:{worker_status.worker_id}"
            self.redis_client.hmset(key, worker_status.model_dump())
            self.redis_client.expire(key, 300)  # 5 minute expiry
            logger.info(f"Registered worker {worker_status.worker_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to register worker {worker_status.worker_id}: {e}")
            return False
    
    def update_worker_heartbeat(self, worker_id: str, current_task: Optional[str] = None) -> bool:
        """Update worker heartbeat."""
        try:
            key = f"{self.workers_key}:{worker_id}"
            updates = {"last_heartbeat": json.dumps({"timestamp": "now"})}
            if current_task:
                updates["current_task"] = current_task
            
            self.redis_client.hmset(key, updates)
            self.redis_client.expire(key, 300)  # 5 minute expiry
            return True
        except Exception as e:
            logger.error(f"Failed to update worker {worker_id} heartbeat: {e}")
            return False
    
    def get_active_workers(self) -> List[str]:
        """Get list of active worker IDs."""
        try:
            pattern = f"{self.workers_key}:*"
            keys = self.redis_client.keys(pattern)
            return [key.split(":")[-1] for key in keys]
        except Exception as e:
            logger.error(f"Failed to get active workers: {e}")
            return []
    
    def create_job(self, job_status: JobStatus) -> bool:
        """Create a new download job."""
        try:
            key = f"{self.jobs_key}:{job_status.job_id}"
            self.redis_client.hmset(key, job_status.model_dump())
            logger.info(f"Created job {job_status.job_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to create job {job_status.job_id}: {e}")
            return False
    
    def update_job_progress(self, job_id: str, completed_increment: int = 0, 
                           failed_increment: int = 0) -> bool:
        """Update job progress."""
        try:
            key = f"{self.jobs_key}:{job_id}"
            if completed_increment > 0:
                self.redis_client.hincrby(key, "completed_files", completed_increment)
            if failed_increment > 0:
                self.redis_client.hincrby(key, "failed_files", failed_increment)
            
            self.redis_client.hset(key, "updated_at", json.dumps({"timestamp": "now"}))
            return True
        except Exception as e:
            logger.error(f"Failed to update job {job_id} progress: {e}")
            return False
    
    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get job status."""
        try:
            key = f"{self.jobs_key}:{job_id}"
            job_data = self.redis_client.hgetall(key)
            if job_data:
                return JobStatus.model_validate(job_data)
            return None
        except Exception as e:
            logger.error(f"Failed to get job {job_id} status: {e}")
            return None
    
    def get_queue_size(self) -> int:
        """Get current queue size."""
        try:
            return self.redis_client.llen(self.task_queue)
        except Exception as e:
            logger.error(f"Failed to get queue size: {e}")
            return -1
    
    def get_failed_queue_size(self) -> int:
        """Get failed queue size."""
        try:
            return self.redis_client.llen(self.failed_queue)
        except Exception as e:
            logger.error(f"Failed to get failed queue size: {e}")
            return -1