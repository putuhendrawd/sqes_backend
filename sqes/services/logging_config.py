import logging
import sys
import os
from datetime import datetime

def setup_main_logging(verbosity_level: int, log_date_str: str, log_dir: str = "logs"):
    """
    Configures the root logger for the main process.
    Logs to both the console and a date-stamped file.
    
    Verbosity levels:
    0 (default): WARNING
    1 (-v):      INFO
    2+ (-vv...): DEBUG
    """
    # Map integer count to logging level
    if verbosity_level == 0:
        log_level = logging.WARNING
    elif verbosity_level == 1:
        log_level = logging.INFO
    else:  # 2 or more
        log_level = logging.DEBUG
    
    # Create log directory
    os.makedirs(log_dir, exist_ok=True)
    
    # --- THIS IS THE NEW FILE NAMING LOGIC ---
    
    # 1. Create the base log name
    base_log_name = f"{log_date_str}.log"
    log_file_path = os.path.join(log_dir, base_log_name)
    
    # 2. Check for conflicts and append (1), (2), etc.
    counter = 1
    while os.path.exists(log_file_path):
        # File exists, create a new name, e.g., "sqes_20230101(1).log"
        log_name_with_counter = f"{log_date_str}({counter}).log"
        log_file_path = os.path.join(log_dir, log_name_with_counter)
        counter += 1
    # --- END NEW LOGIC ---

    # Basic config for the root logger
    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] [%(name)-30s] [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout), 
            logging.FileHandler(log_file_path)  # Use the new, unique path
        ]
    )
    
    # Suppress overly verbose libraries
    logging.captureWarnings(True)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("obspy").setLevel(logging.WARNING)
    
    # Use getLevelName() for Python 3.10 compatibility
    logging.getLogger(__name__).info(
        f"Main logger configured. Level: {logging.getLevelName(log_level)}. Log file: {log_file_path}"
    )
    return log_level, log_file_path

def setup_worker_logging(log_level: int, station_code: str, log_file_path: str = None):
    """
    Configures a unique logger for a worker process.
    
    This is multiprocessing-safe:
    - It logs to console (stdout).
    - It logs to the same file as the main process (if log_file_path is provided).
    - It sets `propagate = False` to avoid conflicts with the root logger.
    """
    logger = logging.getLogger(f"worker.{station_code}")
    logger.setLevel(log_level)
    
    # Prevent logs from bubbling up to the root logger (which isn't configured here)
    logger.propagate = False 
    
    # Add handlers for this worker *only if it doesn't have any*
    if not logger.hasHandlers():
        formatter = logging.Formatter(
            # Format includes station code for easy reading
            f"[%(asctime)s] [{station_code:30s}] [%(levelname)-8s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (if path provided)
        if log_file_path:
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    
    return logger