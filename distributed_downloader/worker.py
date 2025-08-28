"""Worker node for downloading files from Hugging Face."""

import os
import time
import uuid
import signal
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from huggingface_hub import hf_hub_download
from loguru import logger

from .models import DownloadTask, TaskStatus, WorkerStatus
from .redis_client import RedisClient


class WorkerNode:
    """Worker node responsible for downloading files."""
    
    def __init__(self, redis_client: RedisClient, worker_id: Optional[str] = None):
        """Initialize worker node."""
        self.redis_client = redis_client
        self.worker_id = worker_id or str(uuid.uuid4())
        self.is_running = False
        self.current_task: Optional[DownloadTask] = None
        self.tasks_completed = 0
        self.tasks_failed = 0
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Start heartbeat thread
        self.heartbeat_thread = None
        
    def start(self):
        """Start the worker node."""
        logger.info(f"Starting worker {self.worker_id}")
        
        # Register worker
        worker_status = WorkerStatus(
            worker_id=self.worker_id,
            is_active=True,
            last_heartbeat=datetime.utcnow()
        )
        
        if not self.redis_client.register_worker(worker_status):
            logger.error("Failed to register worker")
            return
        
        self.is_running = True
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        
        # Main work loop
        self._work_loop()
    
    def stop(self):
        """Stop the worker node."""
        logger.info(f"Stopping worker {self.worker_id}")
        self.is_running = False
        
        # Wait for current task to complete if any
        if self.current_task:
            logger.info("Waiting for current task to complete...")
            # Give it a moment to finish naturally
            time.sleep(2)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.stop()
    
    def _heartbeat_loop(self):
        """Send periodic heartbeats to Redis."""
        while self.is_running:
            try:
                current_task_id = self.current_task.task_id if self.current_task else None
                self.redis_client.update_worker_heartbeat(self.worker_id, current_task_id)
                time.sleep(30)  # Heartbeat every 30 seconds
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
                time.sleep(5)
    
    def _work_loop(self):
        """Main work loop for processing tasks."""
        logger.info(f"Worker {self.worker_id} starting work loop")
        
        while self.is_running:
            try:
                # Get next task from queue
                task = self.redis_client.dequeue_task(timeout=5)
                
                if task is None:
                    continue  # No task available, continue waiting
                
                self.current_task = task
                logger.info(f"Processing task {task.task_id}: {task.file_path}")
                
                # Update task status to in progress
                self.redis_client.update_task_status(
                    task.task_id, 
                    TaskStatus.IN_PROGRESS, 
                    worker_id=self.worker_id
                )
                
                # Process the task
                success = self._process_task(task)
                
                if success:
                    self.redis_client.update_task_status(
                        task.task_id, 
                        TaskStatus.COMPLETED, 
                        worker_id=self.worker_id
                    )
                    self.tasks_completed += 1
                    
                    # Update job progress
                    if "job_id" in task.metadata:
                        self.redis_client.update_job_progress(
                            task.metadata["job_id"], 
                            completed_increment=1
                        )
                    
                    logger.info(f"Completed task {task.task_id}")
                else:
                    # Task failed, mark as failed and potentially requeue
                    self.redis_client.update_task_status(
                        task.task_id, 
                        TaskStatus.FAILED, 
                        worker_id=self.worker_id,
                        error_message=getattr(task, '_error_message', 'Unknown error')
                    )
                    
                    # Try to requeue for retry
                    requeued = self.redis_client.requeue_failed_task(task)
                    if not requeued:
                        self.tasks_failed += 1
                        # Update job progress for permanent failure
                        if "job_id" in task.metadata:
                            self.redis_client.update_job_progress(
                                task.metadata["job_id"], 
                                failed_increment=1
                            )
                    
                    logger.warning(f"Failed task {task.task_id}")
                
                self.current_task = None
                
            except Exception as e:
                logger.error(f"Error in work loop: {e}")
                if self.current_task:
                    self.redis_client.update_task_status(
                        self.current_task.task_id, 
                        TaskStatus.FAILED, 
                        worker_id=self.worker_id,
                        error_message=str(e)
                    )
                    self.current_task = None
                time.sleep(1)
        
        logger.info(f"Worker {self.worker_id} stopped. Completed: {self.tasks_completed}, Failed: {self.tasks_failed}")
    
    def _process_task(self, task: DownloadTask) -> bool:
        """Process a download task."""
        try:
            # Create local directory if it doesn't exist
            local_path = Path(task.file_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if file already exists and has correct size
            if local_path.exists() and task.file_size:
                existing_size = local_path.stat().st_size
                if existing_size == task.file_size:
                    logger.info(f"File already exists with correct size: {task.file_path}")
                    return True
            
            # Attempt to download using huggingface_hub first
            success = self._download_with_hf_hub(task)
            
            if not success:
                # Fallback to direct HTTP download
                success = self._download_with_requests(task)
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing task {task.task_id}: {e}")
            task._error_message = str(e)
            return False
    
    def _download_with_hf_hub(self, task: DownloadTask) -> bool:
        """Download file using huggingface_hub."""
        try:
            # Extract dataset info from metadata
            original_path = task.metadata.get("original_path", "")
            if not original_path:
                return False
            
            # Use hf_hub_download
            local_path = Path(task.file_path)
            cache_dir = local_path.parent
            
            downloaded_path = hf_hub_download(
                repo_id=task.dataset_name,
                filename=original_path,
                repo_type="dataset",
                cache_dir=str(cache_dir),
                local_files_only=False
            )
            
            # Move to final location if needed
            if downloaded_path != str(local_path):
                import shutil
                shutil.move(downloaded_path, str(local_path))
            
            logger.info(f"Downloaded with hf_hub: {task.file_path}")
            return True
            
        except Exception as e:
            logger.warning(f"HF Hub download failed for {task.task_id}: {e}")
            return False
    
    def _download_with_requests(self, task: DownloadTask) -> bool:
        """Download file using direct HTTP requests."""
        try:
            response = requests.get(task.file_url, stream=True, timeout=300)
            response.raise_for_status()
            
            local_path = Path(task.file_path)
            
            # Download with progress tracking
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if not self.is_running:
                        # Worker is shutting down
                        return False
                    f.write(chunk)
            
            # Verify file size if available
            if task.file_size:
                actual_size = local_path.stat().st_size
                if actual_size != task.file_size:
                    logger.warning(f"Size mismatch for {task.file_path}: expected {task.file_size}, got {actual_size}")
                    # Continue anyway as some repos may have updated files
            
            logger.info(f"Downloaded with requests: {task.file_path}")
            return True
            
        except Exception as e:
            logger.error(f"HTTP download failed for {task.task_id}: {e}")
            task._error_message = str(e)
            return False