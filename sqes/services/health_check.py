import logging
import json
from pathlib import Path
from typing import Dict, Any
from obspy import UTCDateTime
from obspy.clients.fdsn import Client as FDSNClient
from obspy.clients.filesystem.sds import Client as SDSClient
from sqes.services.config_loader import load_config
from sqes.services.db_pool import DBPool

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

    return all_ok