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
from .config import HuggingFaceConfig, AppConfig
from .ssl_config import configure_ssl_bypass, configure_requests_ssl_bypass
from .nas_aggregator import NASAggregator


class WorkerNode:
    """Worker node responsible for downloading files."""
    
    def __init__(self, redis_client: RedisClient, app_config: AppConfig, worker_id: Optional[str] = None):
        """Initialize worker node."""
        self.redis_client = redis_client
        self.hf_config = app_config.huggingface
        self.worker_id = worker_id or str(uuid.uuid4())
        
        # Initialize NAS aggregator
        self.nas_aggregator = NASAggregator(app_config.nas)
        
        # Configure SSL bypass if requested
        if self.hf_config.disable_ssl_verify:
            configure_ssl_bypass()
            configure_requests_ssl_bypass()
            logger.info("SSL verification disabled for downloads")
        
        # Configure hf_transfer for faster downloads if requested
        if self.hf_config.enable_hf_transfer:
            self._setup_hf_transfer()
            logger.info("hf_transfer enabled for faster downloads")
        
        # Configure custom endpoint if provided
        if self.hf_config.endpoint:
            self._setup_hf_endpoint(self.hf_config.endpoint)
            logger.info(f"Using custom Hugging Face endpoint: {self.hf_config.endpoint}")
        
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
        
        # Set up more aggressive signal handling
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
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
        
        # Unregister worker from Redis
        self.redis_client.unregister_worker(self.worker_id)
        logger.info(f"Worker {self.worker_id} has been unregistered")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating immediate shutdown")
        self.is_running = False
        # If we have a current task, mark it as interrupted
        if self.current_task:
            logger.info(f"Interrupting current task {self.current_task.task_id}")
            self.redis_client.update_task_status(
                self.current_task.task_id, 
                TaskStatus.FAILED, 
                worker_id=self.worker_id,
                error_message="Worker shutdown - task interrupted"
            )
            # Requeue the task for retry
            self.redis_client.requeue_failed_task(self.current_task)
        
        # Clean up worker registration immediately
        self.redis_client.unregister_worker(self.worker_id)
        logger.info(f"Worker {self.worker_id} shutdown complete")
        
        # Force exit after cleanup
        import sys
        sys.exit(0)
    
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
                    # Copy to NAS if enabled (and optionally delete original)
                    nas_success = True
                    if self.nas_aggregator.enabled and self.nas_aggregator.config.copy_after_download:
                        original_path = task.metadata.get("original_path", "")
                        if original_path:
                            if self.nas_aggregator.config.delete_after_copy:
                                # Use synchronous copy when deleting to ensure proper cleanup
                                nas_success = self.nas_aggregator.copy_to_nas(
                                    task.file_path,
                                    task.dataset_name,
                                    original_path
                                )
                                if nas_success:
                                    logger.info(f"File copied to NAS and original deleted: {task.file_path}")
                                else:
                                    logger.warning(f"Failed to copy to NAS (original file preserved): {task.file_path}")
                            else:
                                # Use asynchronous copy when keeping original
                                nas_success = self.nas_aggregator.copy_to_nas_async(
                                    task.file_path,
                                    task.dataset_name,
                                    original_path
                                )
                                if not nas_success:
                                    logger.warning(f"Failed to start NAS copy: {task.file_path}")
                    
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
                    
                    nas_message = ""
                    if self.nas_aggregator.enabled:
                        if self.nas_aggregator.config.delete_after_copy:
                            nas_message = " (copied to NAS, original deleted)" if nas_success else " (NAS copy failed, original preserved)"
                        else:
                            nas_message = " (NAS copy initiated)"
                    logger.info(f"Completed task {task.task_id}{nas_message}")
                else:
                    # Task failed, mark as failed and handle retry logic
                    error_message = getattr(task, '_error_message', 'Unknown error')
                    
                    self.redis_client.update_task_status(
                        task.task_id, 
                        TaskStatus.FAILED, 
                        worker_id=self.worker_id,
                        error_message=error_message
                    )
                    
                    # Try to requeue for retry (mandatory 3-retry system)
                    requeued = self.redis_client.requeue_failed_task(task)
                    if requeued:
                        logger.warning(f"Task {task.task_id} failed, requeued for retry {task.attempt_count + 1}/{task.max_retries}: {error_message}")
                    else:
                        # Permanently failed after max retries
                        self.tasks_failed += 1
                        logger.error(f"Task {task.task_id} permanently failed after {task.max_retries} attempts: {error_message}")
                        
                        # Update job progress for permanent failure
                        if "job_id" in task.metadata:
                            self.redis_client.update_job_progress(
                                task.metadata["job_id"], 
                                failed_increment=1
                            )
                
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
            
            # Check if file already exists and verify its integrity
            if local_path.exists():
                if self._verify_file_integrity(local_path, task.file_size):
                    logger.info(f"File already exists and verified: {task.file_path}")
                    return True
                else:
                    logger.warning(f"Existing file failed integrity check, re-downloading: {task.file_path}")
                    local_path.unlink()  # Remove corrupted file
            
            # Attempt to download using huggingface_hub first
            success = self._download_with_hf_hub(task)
            
            if not success:
                # Fallback to direct HTTP download
                success = self._download_with_requests(task)
            
            # Always verify file integrity after successful download
            if success:
                if not self._verify_file_integrity(local_path, task.file_size):
                    logger.error(f"Downloaded file failed integrity check: {task.file_path}")
                    if local_path.exists():
                        local_path.unlink()  # Remove corrupted download
                    task._error_message = "File integrity verification failed"
                    return False
                logger.debug(f"File integrity verified: {task.file_path}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing task {task.task_id}: {e}")
            task._error_message = str(e)
            return False
    
    def _download_with_hf_hub(self, task: DownloadTask) -> bool:
        """Download file using huggingface_hub."""
        try:
            # Check if worker is still running before starting
            if not self.is_running:
                return False
                
            # Extract dataset info from metadata
            original_path = task.metadata.get("original_path", "")
            if not original_path:
                return False
            
            # Use hf_hub_download - it downloads to its own cache, we'll copy to our target
            local_path = Path(task.file_path)
            # Let hf_hub_download use its default cache, don't specify cache_dir
            # We'll copy the file to our desired location afterwards
            
            # Since hf_hub_download doesn't support cancellation well,
            # we run it in a thread and check for interruption
            import threading
            import queue
            result_queue = queue.Queue()
            exception_queue = queue.Queue()
            
            def download_thread():
                try:
                    downloaded_path = hf_hub_download(
                        repo_id=task.dataset_name,
                        filename=original_path,
                        repo_type="dataset",
                        local_files_only=False,
                        token=self.hf_config.token,
                        endpoint=self.hf_config.endpoint
                    )
                    result_queue.put(downloaded_path)
                except Exception as e:
                    exception_queue.put(e)
            
            download_thread = threading.Thread(target=download_thread)
            download_thread.daemon = True
            download_thread.start()
            
            # Poll for completion or interruption
            while download_thread.is_alive():
                if not self.is_running:
                    logger.info(f"HF Hub download interrupted for {task.file_path}")
                    return False
                time.sleep(0.1)  # Check every 100ms
            
            # Check for exceptions
            if not exception_queue.empty():
                raise exception_queue.get()
                
            # Get the result
            if result_queue.empty():
                return False
                
            downloaded_path = result_queue.get()
            
            # Ensure target directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy from HF cache to our target location
            if downloaded_path != str(local_path):
                try:
                    import shutil
                    shutil.copy2(downloaded_path, str(local_path))
                    logger.debug(f"Copied from HF cache: {downloaded_path} -> {local_path}")
                except Exception as e:
                    logger.error(f"Failed to copy from HF cache to target location: {e}")
                    return False
            
            # Verify the file exists at target location
            if not local_path.exists():
                logger.error(f"File not found at target location after HF download: {local_path}")
                return False
            
            logger.info(f"Downloaded with hf_hub: {task.file_path}")
            return True
            
        except Exception as e:
            logger.warning(f"HF Hub download failed for {task.task_id}: {e}")
            return False
    
    def _download_with_requests(self, task: DownloadTask) -> bool:
        """Download file using direct HTTP requests."""
        try:
            # Use verify=False if SSL bypass is configured
            verify_ssl = not self.hf_config.disable_ssl_verify
            response = requests.get(task.file_url, stream=True, timeout=300, verify=verify_ssl)
            response.raise_for_status()
            
            local_path = Path(task.file_path)
            
            # Download with progress tracking
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if not self.is_running:
                        # Worker is shutting down - clean up partial download
                        f.close()
                        if local_path.exists():
                            local_path.unlink()  # Delete partial file
                        logger.info(f"Download cancelled for {task.file_path}")
                        return False
                    f.write(chunk)
            
            
            logger.info(f"Downloaded with requests: {task.file_path}")
            return True
            
        except Exception as e:
            logger.error(f"HTTP download failed for {task.task_id}: {e}")
            task._error_message = str(e)
            return False
    
    def _setup_hf_transfer(self):
        """Configure hf_transfer for faster downloads."""
        try:
            import os
            # Set the environment variable to enable hf_transfer
            os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
            
            # Try to import hf_transfer to verify it's available
            try:
                import hf_transfer
                logger.info(f"hf_transfer version {hf_transfer.__version__} available")
            except ImportError:
                logger.warning("hf_transfer not installed. Install with: pip install huggingface-hub[hf_transfer]")
                logger.warning("Falling back to standard downloads")
                
        except Exception as e:
            logger.error(f"Failed to setup hf_transfer: {e}")
    
    def _setup_hf_endpoint(self, endpoint: str):
        """Configure custom Hugging Face endpoint."""
        try:
            import os
            # Set the environment variable for HF endpoint
            os.environ["HF_ENDPOINT"] = endpoint
            logger.info(f"Set HF_ENDPOINT environment variable to: {endpoint}")
        except Exception as e:
            logger.error(f"Failed to setup HF endpoint: {e}")
    
    def _verify_file_integrity(self, file_path: Path, expected_size: Optional[int] = None) -> bool:
        """Verify file integrity by checking size and basic read test."""
        try:
            if not file_path.exists():
                logger.debug(f"File does not exist: {file_path}")
                return False
            
            # Check if file is empty
            actual_size = file_path.stat().st_size
            if actual_size == 0:
                logger.debug(f"File is empty: {file_path}")
                return False
            
            # Check file size if expected size is provided
            if expected_size is not None and actual_size != expected_size:
                logger.warning(f"File size mismatch for {file_path}: expected {expected_size}, got {actual_size}")
                # For now, we'll be lenient with size mismatches as HF repos may update files
                # But we still want to log the discrepancy
            
            # Basic read test - try to read first few bytes to ensure file is not corrupted
            try:
                with open(file_path, 'rb') as f:
                    # Try to read first 1KB to ensure file is readable
                    chunk = f.read(1024)
                    if not chunk and actual_size > 0:
                        logger.debug(f"File appears corrupted (cannot read content): {file_path}")
                        return False
            except (OSError, IOError) as e:
                logger.debug(f"File read test failed for {file_path}: {e}")
                return False
            
            # Check for common signs of corrupted downloads (HTML error pages, etc.)
            if actual_size < 1024:  # Only check small files that might be error pages
                try:
                    with open(file_path, 'rb') as f:
                        content = f.read().lower()
                        # Check for HTML error pages or other common error indicators
                        if b'<html' in content or b'<!doctype' in content:
                            logger.debug(f"File appears to be an HTML error page: {file_path}")
                            return False
                        if b'access denied' in content or b'403 forbidden' in content:
                            logger.debug(f"File appears to be an access denied error: {file_path}")
                            return False
                except (OSError, IOError, UnicodeDecodeError):
                    # If we can't read/decode the file, but it has the right size, assume it's ok
                    pass
            
            logger.debug(f"File integrity check passed: {file_path} ({actual_size} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Error during file integrity check for {file_path}: {e}")
            return False