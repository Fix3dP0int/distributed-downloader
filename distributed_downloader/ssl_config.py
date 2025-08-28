"""SSL configuration for bypassing certificate verification."""

import requests
import urllib3
from huggingface_hub import configure_http_backend
from loguru import logger


def configure_ssl_bypass():
    """Configure SSL certificate verification bypass for Hugging Face requests."""
    try:
        def backend_factory() -> requests.Session:
            session = requests.Session()
            session.verify = False
            return session

        configure_http_backend(backend_factory=backend_factory)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info("SSL certificate verification disabled for Hugging Face requests")
        
    except Exception as e:
        logger.error(f"Failed to configure SSL bypass: {e}")


def configure_requests_ssl_bypass():
    """Configure SSL bypass for direct requests (fallback downloads)."""
    try:
        # Disable SSL warnings for direct requests
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        logger.info("SSL warnings disabled for direct HTTP requests")
    except Exception as e:
        logger.error(f"Failed to configure requests SSL bypass: {e}")