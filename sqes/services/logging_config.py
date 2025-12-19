import logging
import sys
import os
import re
from datetime import datetime

class WarningMessageFilter(logging.Filter):
    def filter(self, record):
        # 1. Safety Check: Ensure message exists
        if not hasattr(record, 'msg'):
            return True
        
        msg = str(record.msg).strip()

        # 2. SUPPRESSION LIST (Exact phrase matching)
        ignore_phrases = [
            "Channel is missing depth information",
            "FIR normalized",
            "Found more than one matching response",
            "encountered in detrend",
            "The use of this method is deprecated",
            "MatplotlibDeprecationWarning",
            "Inconsistent word order"
        ]
        
        # If the message contains any ignored phrase, KILL IT (Return False)
        if any(phrase in msg for phrase in ignore_phrases):
            return False

        # 3. CLEAN UP (Formatting)
        if record.levelno == logging.WARNING:
            # Handle multi-line warnings (remove source code printout)
            if '\n' in msg:
                msg = msg.split('\n')[0]

            # REGEX: Find ":123: Category: Message"
            match = re.search(r':\d+:\s*([^:]+):\s*(.*)$', msg)
            if match:
                warning_type = match.group(1).strip() # e.g. UserWarning
                warning_text = match.group(2).strip() # e.g. Channel is missing...
                record.msg = f"{warning_type}: {warning_text}"
            
            # Fallback: If regex failed but it looks like a path (starts with /), try simple split
            elif msg.startswith('/') or msg.startswith('C:\\'):
                parts = msg.split(': ')
                if len(parts) >= 2:
                    # Take the last part as the message
                    record.msg = parts[-1]

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

def initialize_worker_logger(log_level: int, log_file_path: str = None):
    """
    Initializes the logging system for a worker process.
    This should be called ONCE at the start of the worker process.
    """
    # 1. Ensure Python's warnings are captured into the logging system
    logging.captureWarnings(True)
    
    # 2. Define the Formatter
    formatter = logging.Formatter(
        "[%(asctime)s] [%(station_code)-30s] [%(levelname)-8s] %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    
    # 3. Create the Filter Instance
    warning_filter = WarningMessageFilter()

    # 4. Helper to create fresh handlers with the FILTER ATTACHED
    handlers = []
    
    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    ch.addFilter(warning_filter)  
    handlers.append(ch)
    
    # File Handler (if path provided)
    if log_file_path:
        fh = logging.FileHandler(log_file_path)
        fh.setFormatter(formatter)
        fh.addFilter(warning_filter) 
        handlers.append(fh)

    # --- LOGGER 1: The Worker's Main Logger ---
    # Use a generic name for the worker logger
    logger = logging.getLogger("worker")
    logger.setLevel(log_level)
    logger.propagate = False
    logger.handlers = []  # Clear existing handlers if any
    for h in handlers:
        logger.addHandler(h)

    # --- LOGGER 2: Python Warnings (py.warnings) ---
    class StationContextFilter(logging.Filter):
        def filter(self, record):
            if not hasattr(record, 'station_code'):
                record.station_code = "SYSTEM"
            return True

    station_context_filter = StationContextFilter()
    for h in handlers:
        h.addFilter(station_context_filter)

    warnings_logger = logging.getLogger("py.warnings")
    warnings_logger.setLevel(logging.WARNING)
    warnings_logger.propagate = False
    warnings_logger.handlers = [] 
    for h in handlers:
        warnings_logger.addHandler(h)

    # --- LOGGER 3: ObsPy Logger ---
    obspy_logger = logging.getLogger("obspy")
    obspy_logger.setLevel(logging.WARNING)
    obspy_logger.propagate = False
    obspy_logger.handlers = [] 
    for h in handlers:
        obspy_logger.addHandler(h)

    return logger

def get_station_logger(station_code: str):
    """
    Returns a LoggerAdapter that injects the station code into log messages.
    Uses the 'worker' logger initialized by initialize_worker_logger.
    """
    logger = logging.getLogger("worker")
    return logging.LoggerAdapter(logger, {"station_code": station_code})