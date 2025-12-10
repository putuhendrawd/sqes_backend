import logging
import json
from pathlib import Path
from typing import Dict, Any
from obspy import UTCDateTime
from obspy.clients.fdsn import Client as FDSNClient
from obspy.clients.filesystem.sds import Client as SDSClient
from .config_loader import load_config
from .db_pool import DBPool

logger = logging.getLogger(__name__)

def _censor_config(config: Dict[str, Any]) -> str:
    """
    Takes a config dictionary, censors 'password', and returns a formatted JSON string.
    """
    censored_config = {}
    for key, value in config.items():
        if "password" in str(key).lower() and value:
            censored_config[key] = f"***{value[-2:]}"
        else:
            censored_config[key] = value
    
    # Return as a pretty-printed JSON string
    return json.dumps(censored_config, indent=2)

def check_configurations():
    """
    Loads all configs, prints them, and checks all external connections.
    Returns True if all checks passed, False otherwise.
    """
    all_ok = True
    
    # --- 1. Load [basic] config ---
    try:
        logger.info("--- [basic] Configuration ---")
        basic_config = load_config(section='basic')
        logger.info(_censor_config(basic_config))
    except Exception as e:
        logger.error(f"Failed to load [basic] config: {e}", exc_info=True)
        return False # This is a fatal error

    # --- 2. Check FDSN Client Connection ---
    waveform_source = basic_config.get('waveform_source', 'fdsn').lower()
    inventory_source = basic_config.get('inventory_source', 'fdsn').lower()
    
    if waveform_source == 'fdsn' or inventory_source == 'fdsn':
        logger.info("--- Checking FDSN Client Connection ---")
        try:
            client_creds = load_config(section='client')
            logger.info(f"[client] Config: {_censor_config(client_creds)}")
            
            fdsn_client = FDSNClient(
                client_creds['url'],
                user=client_creds.get('user'),
                password=client_creds.get('password')
            )
            # Perform a simple, fast test query
            # fdsn_client.get_events(starttime=UTCDateTime(2020,1,1,0,0,0), endtime=UTCDateTime(2020,1,1,0,0,1))
            logger.info("✅ FDSN Client connection: OK")
            
        except Exception as e:
            logger.error(f"❌ FDSN Client connection: FAILED")
            logger.error(f"   Error: {e}")
            all_ok = False
    
    # --- 3. Check Database Connection ---
    if str(basic_config.get('use_database', 'true')).lower():
        logger.info("--- Checking Database Connection ---")
        try:
            db_type = basic_config['use_database']
            db_creds = load_config(section=db_type)
            logger.info(f"[{db_type}] Config: {_censor_config(db_creds)}")
            
            pool = DBPool(**db_creds)
            if not pool.is_db_connected():
                 raise Exception("DBPool.is_db_connected() returned False")
            
            logger.info("✅ Database connection: OK")
            # is_db_connected() logs its own success message
            
        except Exception as e:
            logger.error(f"❌ Database connection: FAILED")
            logger.error(f"   Error: {e}")
            all_ok = False
            
    # --- 4. Check Local Paths ---
    logger.info("--- Checking Local Paths ---")
    if waveform_source == 'sds':
        archive_path_str = basic_config.get('archive_path')
        if not archive_path_str:
            logger.error("❌ SDS 'archive_path': FAILED (key not set in config)")
            all_ok = False
        else:
            archive_path = Path(archive_path_str)
            if not archive_path.is_dir():
                logger.error(f"❌ SDS 'archive_path': FAILED (Not a valid directory: {archive_path})")
                all_ok = False
            else:
                try:
                    sds_client = SDSClient(sds_root=archive_path)
                    logger.info(f"✅ SDS 'archive_path': OK ({archive_path})")
                except Exception as e:
                    logger.error(f"❌ Database connection: FAILED")
                    logger.error(f"   Error: {e}")
                    all_ok = False

    if inventory_source == 'local':
        inventory_path_str = basic_config.get('inventory_path')
        if not inventory_path_str:
            logger.error("❌ Local 'inventory_path': FAILED (key not set in config)")
            all_ok = False
        else:
            inventory_path = Path(inventory_path_str)
            if not inventory_path.is_dir():
                logger.error(f"❌ Local 'inventory_path': FAILED (Not a valid directory: {inventory_path})")
                all_ok = False
            else:
                logger.info(f"✅ Local 'inventory_path': OK ({inventory_path})")

    # --- 5. Check QC Thresholds ---
    logger.info("--- QC Analysis Thresholds ---")
    try:
        from .config_loader import load_qc_thresholds
        from ..analysis.models import DEFAULT_THRESHOLDS
        from configparser import ConfigParser
        import os
        
        # Load thresholds that will be used
        thresholds = load_qc_thresholds()
        
        # Check if config file has [qc_thresholds] section
        module_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(module_path, '..', '..', 'config', 'config.ini')
        
        has_custom_section = False
        custom_params = []
        
        if os.path.exists(config_path):
            parser = ConfigParser()
            parser.read(config_path)
            if parser.has_section('qc_thresholds'):
                has_custom_section = True
                custom_params = parser.options('qc_thresholds')
        
        if has_custom_section:
            logger.info(f"✅ Using QC thresholds from config ({len(custom_params)} custom parameters)")
        else:
            logger.info("ℹ️  Using default QC thresholds (no [qc_thresholds] section in config)")
        
        # Show all threshold parameters
        logger.info("   All QC threshold parameters:")
        
        all_params = sorted(vars(DEFAULT_THRESHOLDS).keys())
        custom_set = set(custom_params)
        
        for param in all_params:
            current_val = getattr(thresholds, param, None)
            default_val = getattr(DEFAULT_THRESHOLDS, param, None)
            
            # Mark if it's customized
            if param in custom_set and current_val != default_val:
                logger.info(f"   • {param} = {current_val} [CUSTOM] (default: {default_val})")
            elif param in custom_set:
                # In config but same as default
                logger.info(f"   • {param} = {current_val} [in config, same as default]")
            else:
                # Using default
                logger.info(f"   • {param} = {current_val}")
        
        # Summary
        if has_custom_section:
            custom_count = sum(1 for p in custom_set if getattr(thresholds, p, None) != getattr(DEFAULT_THRESHOLDS, p, None))
            logger.info(f"   Summary: {custom_count} customized, {len(all_params) - custom_count} using defaults")
        else:
            logger.info("   To customize, add [qc_thresholds] section to config.ini")
            logger.info("   See config/sample_config.ini for all available parameters")

        
    except Exception as e:
        logger.warning(f"⚠️  Could not check QC thresholds: {e}")
        logger.warning("   This is not critical - defaults will be used during processing")

    return all_ok