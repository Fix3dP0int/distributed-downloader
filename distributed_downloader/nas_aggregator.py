"""NAS file aggregation utility for copying downloaded files to centralized storage."""

import os
import shutil
from pathlib import Path
from typing import Optional
from loguru import logger

from .config import NASConfig


class NASAggregator:
    """Handles copying downloaded files to NAS storage."""
    
    def __init__(self, nas_config: NASConfig):
        """Initialize NAS aggregator."""
        self.config = nas_config
        self.enabled = nas_config.enabled and nas_config.path is not None
        
        if self.enabled:
            # Validate NAS path
            self.nas_path = Path(nas_config.path)
            if not self._validate_nas_path():
                logger.error(f"NAS path validation failed: {self.nas_path}")
                self.enabled = False
            else:
                logger.info(f"NAS aggregation enabled - target: {self.nas_path}")
        else:
            logger.info("NAS aggregation disabled")
    
    def _validate_nas_path(self) -> bool:
        """Validate that NAS path exists and is writable."""
        try:
            # Check if path exists
            if not self.nas_path.exists():
                # Try to create the directory
                self.nas_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created NAS directory: {self.nas_path}")
            
            # Test write permissions
            test_file = self.nas_path / ".nas_write_test"
            try:
                test_file.write_text("test")
                test_file.unlink()
                return True
            except Exception as e:
                logger.error(f"NAS path not writable: {e}")
                return False
                
        except Exception as e:
            logger.error(f"NAS path validation error: {e}")
            return False
    
    def copy_to_nas(self, local_file_path: str, dataset_name: str, relative_path: str) -> bool:
        """Copy a downloaded file to NAS storage and optionally delete the original."""
        if not self.enabled:
            return True  # Consider it successful if NAS is disabled
        
        try:
            local_path = Path(local_file_path)
            if not local_path.exists():
                logger.error(f"Local file does not exist for NAS copy: {local_path}")
                logger.error(f"This usually means the download didn't complete properly or file was moved/deleted")
                return False
            
            # Determine NAS destination path
            if self.config.preserve_structure:
                # Preserve the dataset structure: /nas/dataset_name/file/path
                nas_dest = self.nas_path / dataset_name / relative_path
            else:
                # Flat structure: /nas/dataset_name_filename
                filename = Path(relative_path).name
                nas_dest = self.nas_path / f"{dataset_name}_{filename}"
            
            # Create destination directory if needed
            nas_dest.parent.mkdir(parents=True, exist_ok=True)
            
            # Skip if file already exists with same size
            if nas_dest.exists():
                if nas_dest.stat().st_size == local_path.stat().st_size:
                    logger.debug(f"File already exists in NAS with same size: {nas_dest}")
                    # Still try to delete original if configured
                    if self.config.delete_after_copy:
                        return self._delete_original_file(local_path)
                    return True
                else:
                    logger.info(f"Overwriting existing NAS file (size mismatch): {nas_dest}")
            
            # Copy file to NAS
            shutil.copy2(local_path, nas_dest)
            
            # Verify copy
            if nas_dest.exists() and nas_dest.stat().st_size == local_path.stat().st_size:
                logger.info(f"Successfully copied to NAS: {local_path} -> {nas_dest}")
                
                # Delete original file if configured
                if self.config.delete_after_copy:
                    return self._delete_original_file(local_path)
                
                return True
            else:
                logger.error(f"NAS copy verification failed: {nas_dest}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to copy file to NAS: {local_file_path} -> {e}")
            return False
    
    def _delete_original_file(self, local_path: Path) -> bool:
        """Delete the original file after successful NAS copy."""
        try:
            local_path.unlink()
            logger.info(f"Deleted original file to save space: {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete original file {local_path}: {e}")
            return False
    
    def copy_to_nas_async(self, local_file_path: str, dataset_name: str, relative_path: str) -> bool:
        """Copy file to NAS in background (non-blocking)."""
        if not self.enabled:
            return True
        
        try:
            import threading
            
            def copy_worker():
                try:
                    result = self.copy_to_nas(local_file_path, dataset_name, relative_path)
                    if not result:
                        logger.error(f"Async NAS copy failed for: {local_file_path}")
                except Exception as e:
                    logger.error(f"Async NAS copy failed: {e}")
            
            # Start copy in background thread
            copy_thread = threading.Thread(target=copy_worker, daemon=True)
            copy_thread.start()
            
            logger.debug(f"Started async NAS copy: {local_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start async NAS copy: {e}")
            # Fallback to synchronous copy
            return self.copy_to_nas(local_file_path, dataset_name, relative_path)
    
    def get_nas_path(self, dataset_name: str, relative_path: str) -> Optional[Path]:
        """Get the expected NAS path for a file."""
        if not self.enabled:
            return None
        
        if self.config.preserve_structure:
            return self.nas_path / dataset_name / relative_path
        else:
            filename = Path(relative_path).name
            return self.nas_path / f"{dataset_name}_{filename}"
    
    def get_nas_stats(self) -> dict:
        """Get NAS storage statistics."""
        if not self.enabled:
            return {"enabled": False}
        
        try:
            stat = self.nas_path.stat()
            # Get disk usage if possible
            try:
                import shutil
                total, used, free = shutil.disk_usage(self.nas_path)
                return {
                    "enabled": True,
                    "path": str(self.nas_path),
                    "exists": True,
                    "total_space": total,
                    "used_space": used,
                    "free_space": free,
                    "config": {
                        "preserve_structure": self.config.preserve_structure,
                        "copy_after_download": self.config.copy_after_download,
                        "delete_after_copy": self.config.delete_after_copy
                    }
                }
            except:
                return {
                    "enabled": True,
                    "path": str(self.nas_path),
                    "exists": True,
                    "config": {
                        "preserve_structure": self.config.preserve_structure,
                        "copy_after_download": self.config.copy_after_download,
                        "delete_after_copy": self.config.delete_after_copy
                    }
                }
        except Exception as e:
            return {
                "enabled": True,
                "path": str(self.nas_path),
                "exists": False,
                "error": str(e)
            }