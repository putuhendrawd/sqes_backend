import logging
import sys
import os
from datetime import datetime


class ObsPyWarningFilter(logging.Filter):
    """
    Custom filter to suppress duplicate warnings from ObsPy.
    Only allows the first occurrence of each warning type to be logged.
    
    Currently filters:
    - 'FIR normalized' warnings
    - 'computed and reported sensitivities differ' warnings
    """
    def __init__(self):
        super().__init__()
        self.fir_warning_seen = False
        self.sensitivity_warning_seen = False
    
    def filter(self, record):
        message = record.getMessage()
        
        # Check for FIR normalized warning
        if 'FIR normalized' in message:
            if self.fir_warning_seen:
                return False  # Suppress duplicate
            else:
                self.fir_warning_seen = True
                return True  # Allow first occurrence
        
        # Check for sensitivity difference warning
        if 'computed and reported sensitivities differ' in message:
            if self.sensitivity_warning_seen:
                return False  # Suppress duplicate
            else:
                self.sensitivity_warning_seen = True
                return True  # Allow first occurrence
        
        return True  # Allow all other messages

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
    - It also configures the ObsPy logger to write to the log file.
    - It configures py.warnings logger to show which worker generated warnings.
    - It configures the root logger to catch ALL warnings from any module.
    """
    
    # First, configure the root logger to catch warnings from ANY module
    # This ensures we don't miss warnings from numpy, scipy, or other libraries ObsPy uses
    root_logger = logging.getLogger()
    
    # Only configure if it doesn't have handlers yet (avoid duplicates)
    if not root_logger.hasHandlers():
        root_logger.setLevel(logging.WARNING)  # Catch warnings and above
        
        root_formatter = logging.Formatter(
            f"[%(asctime)s] [{station_code:30s}] [%(levelname)-8s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler for root
        root_console_handler = logging.StreamHandler(sys.stdout)
        root_console_handler.setFormatter(root_formatter)
        root_logger.addHandler(root_console_handler)
        
        # File handler for root (if path provided)
        if log_file_path:
            root_file_handler = logging.FileHandler(log_file_path)
            root_file_handler.setFormatter(root_formatter)
            root_logger.addHandler(root_file_handler)
    
    # Now configure the worker-specific logger
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
    
    # Configure ObsPy logger to also write to the log file
    # This captures warnings like "FIR normalized" from ObsPy's internal processing
    obspy_logger = logging.getLogger("obspy")
    obspy_logger.setLevel(logging.WARNING)  # Keep at WARNING level to capture warnings
    obspy_logger.propagate = False  # Don't propagate to root
    
    # Only add handlers if ObsPy logger doesn't have any (avoid duplicates)
    if not obspy_logger.hasHandlers():
        obspy_formatter = logging.Formatter(
            f"[%(asctime)s] [{station_code:30s}] [%(levelname)-8s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler for ObsPy (with its own filter instance)
        obspy_console_handler = logging.StreamHandler(sys.stdout)
        obspy_console_handler.setFormatter(obspy_formatter)
        obspy_console_handler.addFilter(ObsPyWarningFilter())  # Separate instance for console
        obspy_logger.addHandler(obspy_console_handler)
        
        # File handler for ObsPy (with its own filter instance)
        if log_file_path:
            obspy_file_handler = logging.FileHandler(log_file_path)
            obspy_file_handler.setFormatter(obspy_formatter)
            obspy_file_handler.addFilter(ObsPyWarningFilter())  # Separate instance for file
            obspy_logger.addHandler(obspy_file_handler)
    
    # Configure py.warnings logger to show which worker generated Python warnings
    # This is used by logging.captureWarnings(True) to capture warnings from the warnings module
    warnings_logger = logging.getLogger("py.warnings")
    warnings_logger.setLevel(logging.WARNING)
    warnings_logger.propagate = False  # Don't propagate to root
    
    # Only add handlers if warnings logger doesn't have any (avoid duplicates)
    if not warnings_logger.hasHandlers():
        warnings_formatter = logging.Formatter(
            f"[%(asctime)s] [{station_code:30s}] [%(levelname)-8s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler for py.warnings
        warnings_console_handler = logging.StreamHandler(sys.stdout)
        warnings_console_handler.setFormatter(warnings_formatter)
        warnings_logger.addHandler(warnings_console_handler)
        
        # File handler for py.warnings (if path provided)
        if log_file_path:
            warnings_file_handler = logging.FileHandler(log_file_path)
            warnings_file_handler.setFormatter(warnings_formatter)
            warnings_logger.addHandler(warnings_file_handler)
    
    return logger