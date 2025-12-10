import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def create_directory(dir_path: str):
    """
    Creates a directory, including any parent directories, if it does not exist.
    """
    try:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Path checked/created: {dir_path}")
    except OSError as e:
        logger.error(f"Error creating directory {dir_path}: {e}", exc_info=True)