import logging
import sys
import os
import re
from datetime import datetime

class WarningMessageFilter(logging.Filter):
    """
    Custom filter to clean up warning messages from Python's warnings module.
    Removes file paths and source code lines, keeping only the actual warning message.
    """
    def filter(self, record):
        # Process WARNING level messages that might contain file paths
        if record.levelno == logging.WARNING and hasattr(record, 'msg'):
            # The warning message often contains file path and line info
            # Format: "filepath:line: WarningType: actual message\n  source code"
            msg = str(record.msg)
            
            # Split by newlines to remove source code line (e.g., "  warnings.warn(...)")
            lines = msg.split('\n')
            if len(lines) > 1:
                # Multi-line warning - take only the first line
                main_line = lines[0]
            else:
                main_line = msg
            
            # Use regex to extract WarningType and message, ignoring the file path
            # Matches: anything followed by :line_number: WarningType: message
            match = re.search(r':\d+: (\w+): (.*)$', main_line)
            if match:
                warning_type = match.group(1)
                warning_msg = match.group(2)
                record.msg = f"{warning_type}: {warning_msg}"
        
        return True

def setup_main_logging(verbosity_level: int, log_date_str: str, log_dir: str = "logs/log"):
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

    # Basic config for the root logger
    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] [%(name)-30s] [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout), 
            logging.FileHandler(log_file_path)
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
    logging.captureWarnings(True)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)  # Catch warnings and above
    
    # Check if root logger already has handlers (inherited from parent process via fork)
    if root_logger.hasHandlers():
        # Add the WarningMessageFilter to all existing handlers
        for handler in root_logger.handlers:
            # Check if filter is not already added
            if not any(isinstance(f, WarningMessageFilter) for f in handler.filters):
                handler.addFilter(WarningMessageFilter())
    else:
        # No handlers exist, create new ones
        root_formatter = logging.Formatter(
            f"[%(asctime)s] [{station_code:30s}] [%(levelname)-8s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler for root
        root_console_handler = logging.StreamHandler(sys.stdout)
        root_console_handler.setFormatter(root_formatter)
        root_console_handler.addFilter(WarningMessageFilter()) 
        root_logger.addHandler(root_console_handler)
        
        # File handler for root (if path provided)
        if log_file_path:
            root_file_handler = logging.FileHandler(log_file_path)
            root_file_handler.setFormatter(root_formatter)
            root_file_handler.addFilter(WarningMessageFilter())
            root_logger.addHandler(root_file_handler)
    
    # Now configure the worker-specific logger
    logger = logging.getLogger(f"worker.{station_code}")
    logger.setLevel(log_level)
    
    # Prevent logs from bubbling up to the root logger (which isn't configured here)
    logger.propagate = False 
    
    # Add handlers for this worker *only if it doesn't have any*
    if not logger.hasHandlers():
        formatter = logging.Formatter(
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
    obspy_logger = logging.getLogger("obspy")
    obspy_logger.setLevel(logging.WARNING)
    obspy_logger.propagate = False
    
    # Only add handlers if ObsPy logger doesn't have any (avoid duplicates)
    if not obspy_logger.hasHandlers():
        obspy_formatter = logging.Formatter(
            f"[%(asctime)s] [{station_code:30s}] [%(levelname)-8s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler for ObsPy
        obspy_console_handler = logging.StreamHandler(sys.stdout)
        obspy_console_handler.setFormatter(obspy_formatter)
        obspy_console_handler.addFilter(WarningMessageFilter())
        obspy_logger.addHandler(obspy_console_handler)
        
        # File handler for ObsPy
        if log_file_path:
            obspy_file_handler = logging.FileHandler(log_file_path)
            obspy_file_handler.setFormatter(obspy_formatter)
            obspy_file_handler.addFilter(WarningMessageFilter())
            obspy_logger.addHandler(obspy_file_handler)
    else:
        # If handlers exist (e.g. process reuse), ensure they have the filter
        for handler in obspy_logger.handlers:
            if not any(isinstance(f, WarningMessageFilter) for f in handler.filters):
                handler.addFilter(WarningMessageFilter())
    
    # Configure py.warnings logger to show which worker generated Python warnings
    warnings_logger = logging.getLogger("py.warnings")
    warnings_logger.setLevel(logging.WARNING)
    warnings_logger.propagate = False
    
    # Check if warnings logger already has handlers
    if warnings_logger.hasHandlers():
        # Add the WarningMessageFilter to all existing handlers
        for handler in warnings_logger.handlers:
            if not any(isinstance(f, WarningMessageFilter) for f in handler.filters):
                handler.addFilter(WarningMessageFilter())
    else:
        # Only add handlers if warnings logger doesn't have any (avoid duplicates)
        warnings_formatter = logging.Formatter(
            f"[%(asctime)s] [{station_code:30s}] [%(levelname)-8s] %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        
        # Console handler for py.warnings
        warnings_console_handler = logging.StreamHandler(sys.stdout)
        warnings_console_handler.setFormatter(warnings_formatter)
        warnings_console_handler.addFilter(WarningMessageFilter())
        warnings_logger.addHandler(warnings_console_handler)
        
        # File handler for py.warnings (if path provided)
        if log_file_path:
            warnings_file_handler = logging.FileHandler(log_file_path)
            warnings_file_handler.setFormatter(warnings_formatter)
            warnings_file_handler.addFilter(WarningMessageFilter())
            warnings_logger.addHandler(warnings_file_handler)
    
    return logger