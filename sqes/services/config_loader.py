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