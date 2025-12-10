"""Helper functions for workflow processing."""
import os
import time
import logging
import multiprocessing
from obspy import UTCDateTime

from ..services.config_loader import load_config
from ..services.file_system import create_directory

logger = logging.getLogger(__name__)


def get_common_configs(basic_config):
    """Loads all necessary configs.
    
    Args:
        basic_config: Basic configuration dictionary
        
    Returns:
        Tuple of (basic_config, db_type, client_credentials, db_credentials)
    """
    db_type = basic_config['use_database']
    client_credentials = load_config(section='client')
    db_credentials = load_config(section=db_type)
    return basic_config, db_type, client_credentials, db_credentials


def setup_paths_and_times(date_str):
    """Generates paths and time objects for a given date.
    
    Args:
        date_str: Date string in YYYYMMDD format
        
    Returns:
        Tuple of (time0, time1, tgl, tahun) where:
        - time0: UTCDateTime object for start of day
        - time1: UTCDateTime object for end of day
        - tgl: Date string in YYYY-MM-DD format
        - tahun: Year as integer
    """
    wkt1 = time.strptime(date_str, "%Y%m%d")
    time0 = UTCDateTime(date_str)
    tgl = time0.strftime("%Y-%m-%d")
    time1 = time0 + 86400
    tahun = wkt1.tm_year
    
    return time0, time1, tgl, tahun


def get_output_paths(basic_config, tahun, tgl, date_str):
    """Creates and returns a dictionary of output paths.
    
    All paths use YYYY-MM-DD format for consistency.
    
    Args:
        basic_config: Basic configuration dictionary
        tahun: Year as integer (unused, kept for compatibility)
        tgl: Date string in YYYY-MM-DD format
        date_str: Date string in YYYYMMDD format (unused, kept for compatibility)
        
    Returns:
        Dictionary with keys: outputPSD, outputPDF, outputsignal, outputmseed
    """
    outputPSD = os.path.join(basic_config['outputpsd'], str(tgl))
    outputPDF = os.path.join(basic_config['outputpdf'], str(tgl))
    outputsignal = os.path.join(basic_config['outputsignal'], str(tgl))
    outputmseed = os.path.join(basic_config['outputmseed'], str(tgl))
    
    create_directory(outputsignal)
    create_directory(outputPDF)
    create_directory(outputmseed)
    create_directory(outputPSD)
    
    return {
        'outputPSD': outputPSD,
        'outputPDF': outputPDF,
        'outputsignal': outputsignal,
        'outputmseed': outputmseed
    }


def calculate_process_count(x, base=2):
    """Calculates the number of processes to use for multiprocessing.
    
    Args:
        x: Suggested number of processes
        base: Rounding base (default: 2)
        
    Returns:
        Integer number of processes, clamped between 1 and max_cpus/3
    """
    min_value = 4
    # Use max(1, ...) to avoid 0 CPUs on small machines
    max_value = max(1, multiprocessing.cpu_count() // 3)
    rounded_value = base * round(x / base)
    
    # Clamp the value between min and max
    rounded = max(1, min(max_value, max(min_value, rounded_value)))
    return rounded


def load_qc_thresholds():
    """Load QC thresholds from config file with fallback to defaults.
    
    This function loads the QC analysis thresholds from the config file.
    If the config file doesn't exist or the [qc_thresholds] section is missing,
    it will return the default thresholds.
    
    Returns:
        QCThresholds object with values from config or defaults
    """
    from ..services.config_loader import load_qc_thresholds as _load_qc_thresholds
    
    try:
        thresholds = _load_qc_thresholds()
        logger.debug("Loaded QC thresholds from config")
        return thresholds
    except Exception as e:
        logger.warning(f"Failed to load QC thresholds: {e}. Using defaults.")
        from ..analysis.models import DEFAULT_THRESHOLDS
        return DEFAULT_THRESHOLDS


