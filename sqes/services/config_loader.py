# sqes/services/config_loader.py
import os
import logging
from configparser import ConfigParser
from typing import Dict, Any

logger = logging.getLogger(__name__)

def load_config(filename: str = 'config.ini', section: str = 'postgresql') -> Dict[str, Any]:
    """
    Loads a specific section from the config.ini file.
    
    Args:
        filename (str): The name of the config file (default: 'config.ini').
        section (str): The [section] in the INI file to load.

    Returns:
        Dict[str, Any]: A dictionary of the settings.
        
    Raises:
        FileNotFoundError: If the config.ini file cannot be found.
        Exception: If the specified section is not found in the file.
    """
    module_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(module_path, '..', '..', 'config', filename)
    
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found at: {config_path}")
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
    parser = ConfigParser()
    parser.read(config_path)
    
    config: Dict[str, Any] = {}
    if parser.has_section(section):
        
        # --- THIS IS THE FIX ---
        # Define all keys that should be converted to integers
        int_keys = {'cpu_number_used', 'pool_size'}
        # --- END FIX ---

        params = parser.items(section)
        for param in params:
            key = param[0]
            value = param[1]

            # --- UPDATED LOGIC ---
            if key in int_keys and value:
                try:
                    config[key] = int(value)
                except ValueError:
                    logger.warning(f"Invalid integer value for '{key}': {value}. Using None.")
                    config[key] = None
            else:
                config[key] = value
            # --- END UPDATED LOGIC ---
    else:
        logger.error(f"Section '{section}' not found in the {config_path} file")
        raise Exception(f"Section '{section}' not found in the {config_path} file")
    
    return config


def load_qc_thresholds(filename: str = 'config.ini'):
    """
    Loads QC thresholds from the config file.
    
    If the [qc_thresholds] section doesn't exist or any parameter is missing,
    the corresponding default value will be used.
    
    Args:
        filename (str): The name of the config file (default: 'config.ini').
        
    Returns:
        QCThresholds: QCThresholds object with values from config or defaults
    """
    from ..analysis.models import QCThresholds, DEFAULT_THRESHOLDS
    
    module_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(module_path, '..', '..', 'config', filename)
    
    # If config doesn't exist, return defaults
    if not os.path.exists(config_path):
        logger.info(f"Config file not found at {config_path}, using default QC thresholds")
        return DEFAULT_THRESHOLDS
        
    parser = ConfigParser()
    parser.read(config_path)
    
    # If qc_thresholds section doesn't exist, return defaults
    if not parser.has_section('qc_thresholds'):
        logger.debug("No [qc_thresholds] section in config, using defaults")
        return DEFAULT_THRESHOLDS
    
    # Build kwargs from config, only including parameters that exist
    kwargs = {}
    
    # Define all possible threshold parameters with their types
    float_params = [
        'rms_limit', 'ratioamp_limit', 'gap_limit', 'overlap_limit', 'spike_limit',
        'rms_margin', 'ratioamp_margin', 'gap_margin', 'overlap_margin', 'spike_margin',
        'pct_below_warn', 'pct_above_warn', 'avail_good', 'avail_fair', 
        'avail_min_for_noise_check', 'dcl_dead', 'rms_damaged_max',
        'fair_max_score', 'poor_max_score', 'weight_noise', 'weight_availability',
        'weight_rms', 'weight_ratioamp', 'weight_gaps', 'weight_overlaps', 'weight_spikes'
    ]
    
    int_params = ['gap_count_warn', 'overlap_count_warn', 'spike_count_warn']
    
    # Load float parameters
    for param in float_params:
        if parser.has_option('qc_thresholds', param):
            try:
                kwargs[param] = parser.getfloat('qc_thresholds', param)
            except ValueError as e:
                logger.warning(f"Invalid float value for '{param}': {e}. Using default.")
    
    # Load integer parameters
    for param in int_params:
        if parser.has_option('qc_thresholds', param):
            try:
                kwargs[param] = parser.getint('qc_thresholds', param)
            except ValueError as e:
                logger.warning(f"Invalid integer value for '{param}': {e}. Using default.")
    
    # Create dict with all defaults
    default_dict = {
        'rms_limit': DEFAULT_THRESHOLDS.rms_limit,
        'ratioamp_limit': DEFAULT_THRESHOLDS.ratioamp_limit,
        'gap_limit': DEFAULT_THRESHOLDS.gap_limit,
        'overlap_limit': DEFAULT_THRESHOLDS.overlap_limit,
        'spike_limit': DEFAULT_THRESHOLDS.spike_limit,
        'rms_margin': DEFAULT_THRESHOLDS.rms_margin,
        'ratioamp_margin': DEFAULT_THRESHOLDS.ratioamp_margin,
        'gap_margin': DEFAULT_THRESHOLDS.gap_margin,
        'overlap_margin': DEFAULT_THRESHOLDS.overlap_margin,
        'spike_margin': DEFAULT_THRESHOLDS.spike_margin,
        'pct_below_warn': DEFAULT_THRESHOLDS.pct_below_warn,
        'pct_above_warn': DEFAULT_THRESHOLDS.pct_above_warn,
        'gap_count_warn': DEFAULT_THRESHOLDS.gap_count_warn,
        'overlap_count_warn': DEFAULT_THRESHOLDS.overlap_count_warn,
        'spike_count_warn': DEFAULT_THRESHOLDS.spike_count_warn,
        'avail_good': DEFAULT_THRESHOLDS.avail_good,
        'avail_fair': DEFAULT_THRESHOLDS.avail_fair,
        'avail_min_for_noise_check': DEFAULT_THRESHOLDS.avail_min_for_noise_check,
        'dcl_dead': DEFAULT_THRESHOLDS.dcl_dead,
        'rms_damaged_max': DEFAULT_THRESHOLDS.rms_damaged_max,
        'fair_max_score': DEFAULT_THRESHOLDS.fair_max_score,
        'poor_max_score': DEFAULT_THRESHOLDS.poor_max_score,
        'weight_noise': DEFAULT_THRESHOLDS.weight_noise,
        'weight_availability': DEFAULT_THRESHOLDS.weight_availability,
        'weight_rms': DEFAULT_THRESHOLDS.weight_rms,
        'weight_ratioamp': DEFAULT_THRESHOLDS.weight_ratioamp,
        'weight_gaps': DEFAULT_THRESHOLDS.weight_gaps,
        'weight_overlaps': DEFAULT_THRESHOLDS.weight_overlaps,
        'weight_spikes': DEFAULT_THRESHOLDS.weight_spikes,
    }
    
    # Override defaults with config values
    default_dict.update(kwargs)
    
    logger.info(f"Loaded QC thresholds from {config_path} ({len(kwargs)} custom parameters)")
    return QCThresholds(**default_dict)