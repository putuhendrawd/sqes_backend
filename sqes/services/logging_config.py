# sqes/infrastructure/logging_config.py
import logging
import sys
import os
from datetime import datetime

def setup_main_logging(verbosity_level: int, log_dir: str = "logs"):
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
    log_file = os.path.join(log_dir, f"sqes_{datetime.now().strftime('%Y%m%d')}.log")

    # Basic config for the root logger
    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] [%(name)-20s] [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout), # Console handler
            logging.FileHandler(log_file)      # File handler
        ]
    )
    
    # Suppress overly verbose libraries
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("obspy").setLevel(logging.WARNING)
    
    # Log the configured level using getLevelName()
    logging.getLogger(__name__).info(
        f"Main logger configured. Level: {logging.getLevelName(log_level)}. Log file: {log_file}"
    )
    return log_level # Return the level for workers

def setup_worker_logging(log_level: int, station_code: str):
    """
    Configures a unique logger for a worker process.
    
    This is multiprocessing-safe:
    - It logs to console (stdout).
    - It sets `propagate = False` to avoid conflicts with the root logger.
    """
    logger = logging.getLogger(f"worker.{station_code}")
    logger.setLevel(log_level)
    
    # Prevent logs from bubbling up to the root logger (which isn't configured here)
    logger.propagate = False 
    
    # Add a console handler for this worker *only if it doesn't have one*
    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            # Format includes station code for easy reading
            f"[%(asctime)s] [{station_code:5s}] [%(levelname)-8s] %(message)s",
            "%H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger