"""Master node for coordinating distributed downloads."""

import uuid
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from huggingface_hub import HfApi, HfFileSystem
from loguru import logger

from .models import DownloadTask, TaskStatus, JobStatus
from .redis_client import RedisClient
from .config import HuggingFaceConfig
from .ssl_config import configure_ssl_bypass


class MasterNode:
    """Master node responsible for task creation and coordination."""
    
    def __init__(self, redis_client: RedisClient, hf_config: HuggingFaceConfig):
        """Initialize master node."""
        self.redis_client = redis_client
        self.hf_config = hf_config
        
        # Configure SSL bypass if requested
        if hf_config.disable_ssl_verify:
            configure_ssl_bypass()
            logger.info("SSL verification disabled for Hugging Face API")
        
        # Configure custom endpoint if provided
        if hf_config.endpoint:
            self._setup_hf_endpoint(hf_config.endpoint)
            logger.info(f"Using custom Hugging Face endpoint: {hf_config.endpoint}")
        
        # Initialize HF API with token and endpoint
        token = hf_config.token
        endpoint = hf_config.endpoint
        self.hf_api = HfApi(token=token, endpoint=endpoint)
        self.hf_fs = HfFileSystem(token=token, endpoint=endpoint)
        
    def create_download_job(self, dataset_name: str, local_path: Optional[str] = None) -> Optional[str]:
        """Create a new download job for a Hugging Face dataset."""
        try:
            job_id = str(uuid.uuid4())
            logger.info(f"Creating download job {job_id} for dataset: {dataset_name}")
            
            # Get dataset info
            try:
                repo_info = self.hf_api.repo_info(repo_id=dataset_name, repo_type="dataset")
                if not repo_info:
                    logger.error(f"Dataset {dataset_name} not found")
                    return None
            except Exception as e:
                logger.error(f"Failed to get dataset info for {dataset_name}: {e}")
                return None
            
            # List all files in the dataset
            files = self._list_dataset_files(dataset_name)
            if not files:
                logger.error(f"No files found for dataset {dataset_name}")
                return None
                
            logger.info(f"Found {len(files)} files in dataset {dataset_name}")
            
            # Create job status
            job_status = JobStatus(
                job_id=job_id,
                dataset_name=dataset_name,
                total_files=len(files),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                status=TaskStatus.PENDING
            )
            
            if not self.redis_client.create_job(job_status):
                logger.error(f"Failed to create job {job_id} in Redis")
                return None
            
            # Create download tasks
            tasks_created = 0
            for file_info in files:
                task = self._create_download_task(
                    job_id=job_id,
                    dataset_name=dataset_name,
                    file_info=file_info,
                    local_path=local_path
                )
                
                if task and self.redis_client.enqueue_task(task):
                    tasks_created += 1
                else:
                    logger.warning(f"Failed to create/enqueue task for file: {file_info['path']}")
            
            logger.info(f"Successfully created {tasks_created}/{len(files)} tasks for job {job_id}")
            
            # Update job status to in progress
            job_status.status = TaskStatus.IN_PROGRESS
            self.redis_client.create_job(job_status)  # Update with new status
            
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to create download job for {dataset_name}: {e}")
            return None
    
    def _list_dataset_files(self, dataset_name: str) -> List[dict]:
        """List all files in a Hugging Face dataset."""
        try:
            files = []
            # Use HfFileSystem to list files
            file_paths = self.hf_fs.find(f"datasets/{dataset_name}")
            
            for file_path in file_paths:
                if not self.hf_fs.isfile(file_path):
                    continue  # Skip directories
                    
                # Get file info
                try:
                    file_stat = self.hf_fs.stat(file_path)
                    relative_path = file_path.replace(f"datasets/{dataset_name}/", "")
                    
                    files.append({
                        "path": relative_path,
                        "full_path": file_path,
                        "size": file_stat.get("size", 0)
                    })
                except Exception as e:
                    logger.warning(f"Failed to get stat for {file_path}: {e}")
                    # Add without size info
                    relative_path = file_path.replace(f"datasets/{dataset_name}/", "")
                    files.append({
                        "path": relative_path,
                        "full_path": file_path,
                        "size": None
                    })
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files for dataset {dataset_name}: {e}")
            return []
    
    def _create_download_task(self, job_id: str, dataset_name: str, 
                            file_info: dict, local_path: Optional[str] = None) -> Optional[DownloadTask]:
        """Create a download task for a specific file."""
        try:
            task_id = str(uuid.uuid4())
            
            # Construct the download URL using configured endpoint
            base_url = self.hf_config.endpoint if self.hf_config.endpoint else "https://huggingface.co"
            file_url = f"{base_url}/datasets/{dataset_name}/resolve/main/{file_info['path']}"
            
            # Determine local file path
            if local_path:
                local_file_path = str(Path(local_path) / dataset_name / file_info["path"])
            else:
                local_file_path = str(Path("./downloads") / dataset_name / file_info["path"])
            
            task = DownloadTask(
                task_id=task_id,
                dataset_name=dataset_name,
                file_path=local_file_path,
                file_url=file_url,
                file_size=file_info.get("size"),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                metadata={
                    "job_id": job_id,
                    "original_path": file_info["path"]
                }
            )
            
            return task
            
        except Exception as e:
            logger.error(f"Failed to create download task for {file_info.get('path', 'unknown')}: {e}")
            return None
    
    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get the status of a download job."""
        return self.redis_client.get_job_status(job_id)
    
    def list_active_workers(self) -> List[str]:
        """Get list of active workers."""
        return self.redis_client.get_active_workers()
    
    def get_queue_stats(self) -> dict:
        """Get queue statistics."""
        return {
            "pending_tasks": self.redis_client.get_queue_size(),
            "failed_tasks": self.redis_client.get_failed_queue_size(),
            "active_workers": len(self.redis_client.get_active_workers())
        }
    
    def _setup_hf_endpoint(self, endpoint: str):
        """Configure custom Hugging Face endpoint."""
        try:
            import os
            # Set the environment variable for HF endpoint
            os.environ["HF_ENDPOINT"] = endpoint
            logger.info(f"Set HF_ENDPOINT environment variable to: {endpoint}")
        except Exception as e:
            logger.error(f"Failed to setup HF endpoint: {e}")