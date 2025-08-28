"""Configuration management for the distributed downloader."""

import os
import configparser
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass

from loguru import logger


@dataclass
class RedisConfig:
    """Redis configuration settings."""
    host: str = "localhost"
    port: int = 6379
    password: Optional[str] = None
    username: Optional[str] = None
    db: int = 0


@dataclass
class HuggingFaceConfig:
    """Hugging Face configuration settings."""
    token: Optional[str] = None
    cache_dir: Optional[str] = None


@dataclass
class AppConfig:
    """Application configuration."""
    redis: RedisConfig
    huggingface: HuggingFaceConfig
    log_level: str = "INFO"
    output_dir: Optional[str] = None


class ConfigManager:
    """Manages configuration from files, environment variables, and CLI args."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration manager."""
        self.config_file = config_file or self._find_config_file()
        self.config = self._load_config()
    
    def _find_config_file(self) -> Optional[str]:
        """Find configuration file in standard locations."""
        possible_paths = [
            "config.ini",
            "distributed_downloader.ini",
            "~/.config/distributed_downloader/config.ini",
            "~/.distributed_downloader.ini",
            "/etc/distributed_downloader/config.ini"
        ]
        
        for path_str in possible_paths:
            path = Path(path_str).expanduser()
            if path.exists():
                logger.info(f"Found config file: {path}")
                return str(path)
        
        logger.info("No config file found, using defaults")
        return None
    
    def _load_config(self) -> AppConfig:
        """Load configuration from file and environment variables."""
        # Start with defaults
        redis_config = RedisConfig()
        hf_config = HuggingFaceConfig()
        log_level = "INFO"
        output_dir = None
        
        # Load from config file if available
        if self.config_file and Path(self.config_file).exists():
            parser = configparser.ConfigParser()
            try:
                parser.read(self.config_file)
                
                # Redis configuration
                if "redis" in parser:
                    redis_section = parser["redis"]
                    redis_config.host = redis_section.get("host", redis_config.host)
                    redis_config.port = redis_section.getint("port", redis_config.port)
                    redis_config.password = redis_section.get("password", redis_config.password)
                    redis_config.username = redis_section.get("username", redis_config.username)
                    redis_config.db = redis_section.getint("db", redis_config.db)
                
                # Hugging Face configuration
                if "huggingface" in parser:
                    hf_section = parser["huggingface"]
                    hf_config.token = hf_section.get("token", hf_config.token)
                    hf_config.cache_dir = hf_section.get("cache_dir", hf_config.cache_dir)
                
                # App configuration
                if "app" in parser:
                    app_section = parser["app"]
                    log_level = app_section.get("log_level", log_level)
                    output_dir = app_section.get("output_dir", output_dir)
                
                logger.info(f"Loaded configuration from {self.config_file}")
                
            except Exception as e:
                logger.warning(f"Error reading config file {self.config_file}: {e}")
        
        # Override with environment variables
        redis_config.host = os.getenv("REDIS_HOST", redis_config.host)
        redis_config.port = int(os.getenv("REDIS_PORT", redis_config.port))
        redis_config.password = os.getenv("REDIS_PASSWORD", redis_config.password)
        redis_config.username = os.getenv("REDIS_USERNAME", redis_config.username)
        redis_config.db = int(os.getenv("REDIS_DB", redis_config.db))
        
        # Hugging Face token from environment
        hf_token = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_HUB_TOKEN")
        if hf_token:
            hf_config.token = hf_token
        
        hf_config.cache_dir = os.getenv("HF_CACHE_DIR", hf_config.cache_dir)
        
        # App settings from environment
        log_level = os.getenv("LOG_LEVEL", log_level)
        output_dir = os.getenv("OUTPUT_DIR", output_dir)
        
        return AppConfig(
            redis=redis_config,
            huggingface=hf_config,
            log_level=log_level,
            output_dir=output_dir
        )
    
    def get_config(self) -> AppConfig:
        """Get the current configuration."""
        return self.config
    
    def update_from_cli_args(self, **kwargs):
        """Update configuration with CLI arguments."""
        for key, value in kwargs.items():
            if value is not None:
                if key in ["redis_host", "redis_port", "redis_password", "redis_username"]:
                    redis_attr = key.replace("redis_", "")
                    setattr(self.config.redis, redis_attr, value)
                elif key in ["hf_token", "huggingface_token"]:
                    self.config.huggingface.token = value
                elif key == "log_level":
                    self.config.log_level = value
                elif key == "output_dir":
                    self.config.output_dir = value
    
    def create_sample_config(self, file_path: str):
        """Create a sample configuration file."""
        config = configparser.ConfigParser()
        
        # Redis section
        config["redis"] = {
            "host": "localhost",
            "port": "6379", 
            "password": "# your_redis_password",
            "username": "# your_redis_username (Redis 6.0+ ACL)",
            "db": "0"
        }
        
        # Hugging Face section
        config["huggingface"] = {
            "token": "# your_huggingface_token",
            "cache_dir": "# /path/to/cache/dir (optional)"
        }
        
        # App section
        config["app"] = {
            "log_level": "INFO",
            "output_dir": "# /path/to/downloads (optional)"
        }
        
        with open(file_path, 'w') as f:
            f.write("# Distributed Downloader Configuration\n")
            f.write("# Lines starting with # are comments\n")
            f.write("# Remove the # to uncomment settings\n\n")
            config.write(f)
        
        logger.info(f"Created sample config file: {file_path}")


# Global config manager instance
_config_manager: Optional[ConfigManager] = None


def get_config_manager(config_file: Optional[str] = None) -> ConfigManager:
    """Get the global configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(config_file)
    return _config_manager


def get_config(config_file: Optional[str] = None) -> AppConfig:
    """Get the current configuration."""
    return get_config_manager(config_file).get_config()