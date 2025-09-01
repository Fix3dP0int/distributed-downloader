"""Redis client for message queue operations."""

import json
import redis
from datetime import datetime
from typing import Optional, List, Dict, Any
from loguru import logger

from .models import DownloadTask, TaskStatus, WorkerStatus, JobStatus


class RedisClient:
    """Redis client for managing download tasks and worker coordination."""
    
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, 
                 password: Optional[str] = None, username: Optional[str] = None):
        """Initialize Redis client."""
        # For Redis 6.0+ ACL support, use username if provided
        if username and password:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                username=username,
                password=password,
                decode_responses=True
            )
        else:
            # Traditional password-only authentication
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
        self.repos_key = "downloader:repos"
        self.repo_list_key = "downloader:repo_list"
        
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
                "updated_at": datetime.utcnow().isoformat(),
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
        """Requeue a failed task for retry with mandatory 3-retry system."""
        try:
            task.attempt_count += 1
            task.status = TaskStatus.RETRYING
            task.updated_at = datetime.utcnow()
            
            if task.attempt_count <= task.max_retries:
                logger.info(f"Requeuing task {task.task_id} for retry {task.attempt_count}/{task.max_retries}")
                return self.enqueue_task(task)
            else:
                # Exceeded max retries - permanently failed
                task.status = TaskStatus.FAILED
                task_data = task.model_dump_json()
                self.redis_client.lpush(self.failed_queue, task_data)
                logger.error(f"Task {task.task_id} permanently failed after {task.max_retries} retries")
                return False
        except Exception as e:
            logger.error(f"Failed to requeue task {task.task_id}: {e}")
            return False
    
    def register_worker(self, worker_status: WorkerStatus) -> bool:
        """Register a worker."""
        try:
            key = f"{self.workers_key}:{worker_status.worker_id}"
            # Convert model to dict and serialize fields for Redis
            worker_data = worker_status.model_dump()
            
            # Create a new dict with properly serialized values
            redis_data = {}
            
            for field, value in worker_data.items():
                if value is None:
                    # Skip None values or convert to empty string
                    redis_data[field] = ""
                elif isinstance(value, datetime):
                    redis_data[field] = value.isoformat()
                elif isinstance(value, bool):
                    redis_data[field] = str(value).lower()
                elif isinstance(value, dict):
                    redis_data[field] = json.dumps(value)
                elif isinstance(value, (int, float)):
                    redis_data[field] = str(value)
                else:
                    redis_data[field] = str(value)
            
            self.redis_client.hmset(key, redis_data)
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
            updates = {"last_heartbeat": datetime.utcnow().isoformat()}
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
    
    def unregister_worker(self, worker_id: str) -> bool:
        """Unregister a worker (remove from Redis)."""
        try:
            key = f"{self.workers_key}:{worker_id}"
            result = self.redis_client.delete(key)
            if result:
                logger.info(f"Unregistered worker {worker_id}")
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to unregister worker {worker_id}: {e}")
            return False
    
    def create_job(self, job_status: JobStatus) -> bool:
        """Create a new download job."""
        try:
            key = f"{self.jobs_key}:{job_status.job_id}"
            # Convert model to dict and serialize datetime fields
            job_data = job_status.model_dump()
            job_data['created_at'] = job_data['created_at'].isoformat() if isinstance(job_data['created_at'], datetime) else job_data['created_at']
            job_data['updated_at'] = job_data['updated_at'].isoformat() if isinstance(job_data['updated_at'], datetime) else job_data['updated_at']
            
            self.redis_client.hmset(key, job_data)
            
            # Create repository indexing if dataset_name exists
            if hasattr(job_status, 'dataset_name') and job_status.dataset_name:
                self._index_repository(job_status.dataset_name, job_status.job_id, job_data)
            
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
            
            self.redis_client.hset(key, "updated_at", datetime.utcnow().isoformat())
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
                # Convert string datetime fields back to datetime objects
                if 'created_at' in job_data and isinstance(job_data['created_at'], str):
                    job_data['created_at'] = datetime.fromisoformat(job_data['created_at'])
                if 'updated_at' in job_data and isinstance(job_data['updated_at'], str):
                    job_data['updated_at'] = datetime.fromisoformat(job_data['updated_at'])
                    
                # Convert string numbers back to int
                if 'total_files' in job_data:
                    job_data['total_files'] = int(job_data['total_files'])
                if 'completed_files' in job_data:
                    job_data['completed_files'] = int(job_data['completed_files'])
                if 'failed_files' in job_data:
                    job_data['failed_files'] = int(job_data['failed_files'])
                if 'active_workers' in job_data:
                    job_data['active_workers'] = int(job_data['active_workers'])
                    
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
    
    def _normalize_repo_name(self, repo_name: str) -> str:
        """Convert repo name to Redis-safe format (replace '/' with '__')."""
        return repo_name.replace('/', '__')
    
    def _denormalize_repo_name(self, normalized_name: str) -> str:
        """Convert Redis key back to original repo name."""
        return normalized_name.replace('__', '/')
    
    def _index_repository(self, repo_name: str, job_id: str, job_data: dict) -> bool:
        """Create repository index for easy lookup."""
        try:
            normalized_name = self._normalize_repo_name(repo_name)
            repo_key = f"{self.repos_key}:{normalized_name}"
            
            # Store repository metadata
            repo_data = {
                "repo_name": repo_name,
                "job_id": job_id,
                "status": job_data.get('status', 'PENDING'),
                "total_files": job_data.get('total_files', 0),
                "created_at": job_data.get('created_at'),
                "updated_at": job_data.get('updated_at')
            }
            
            self.redis_client.hmset(repo_key, repo_data)
            # Add to repository list
            self.redis_client.sadd(self.repo_list_key, repo_name)
            
            logger.debug(f"Indexed repository: {repo_name} -> {job_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to index repository {repo_name}: {e}")
            return False
    
    def get_job_by_repo(self, repo_name: str) -> Optional[str]:
        """Get job ID by repository name."""
        try:
            normalized_name = self._normalize_repo_name(repo_name)
            repo_key = f"{self.repos_key}:{normalized_name}"
            job_id = self.redis_client.hget(repo_key, "job_id")
            return job_id
        except Exception as e:
            logger.error(f"Failed to get job for repo {repo_name}: {e}")
            return None
    
    def list_repositories(self) -> List[str]:
        """Get list of all repositories."""
        try:
            repos = self.redis_client.smembers(self.repo_list_key)
            return list(repos) if repos else []
        except Exception as e:
            logger.error(f"Failed to list repositories: {e}")
            return []
    
    def get_repo_status(self, repo_name: str) -> Optional[dict]:
        """Get repository status information."""
        try:
            normalized_name = self._normalize_repo_name(repo_name)
            repo_key = f"{self.repos_key}:{normalized_name}"
            repo_data = self.redis_client.hgetall(repo_key)
            
            if repo_data:
                # Get the actual job status and sync it
                job_id = repo_data.get('job_id')
                if job_id:
                    job_status = self.get_job_status(job_id)
                    if job_status:
                        # Update the repository data with current job status
                        updated_repo_data = {
                            'completed_files': job_status.completed_files,
                            'failed_files': job_status.failed_files,
                            'total_files': job_status.total_files,
                            'status': job_status.status.value if hasattr(job_status.status, 'value') else str(job_status.status),
                            'updated_at': datetime.utcnow().isoformat()
                        }
                        
                        # Update the repository record in Redis with latest data
                        self.redis_client.hmset(repo_key, updated_repo_data)
                        
                        # Merge with existing repo data
                        repo_data.update(updated_repo_data)
                        
                        logger.debug(f"Synced repo status for {repo_name}: {job_status.completed_files}/{job_status.total_files} files")
                
                return repo_data
            return None
        except Exception as e:
            logger.error(f"Failed to get repo status for {repo_name}: {e}")
            return None