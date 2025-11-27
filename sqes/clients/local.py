# sqes/clients/local_inventory.py
import logging
from pathlib import Path
from typing import Optional
from obspy import UTCDateTime, read_inventory, Inventory
import warnings

logger = logging.getLogger(__name__)

def get_inventory(inventory_path: str, net: str, sta: str,
                    loc: str, cha: str, time0: UTCDateTime) -> Optional[Inventory]:
    """
    Attempts to read inventory data from a local directory.
    Looks for common filenames like NET.STA.xml or NET.STA.dataless.
    """
    inv_root = Path(inventory_path)
    
    # List of common filenames to try
    filenames_to_try = [
        f"{net}.{sta}.xml",
        f"{net}.{sta}.dataless",
        f"{sta}.xml"
    ]

    inv = None
    for filename in filenames_to_try:
        file_path = inv_root / filename
        if file_path.exists():
            try:
                logger.debug(f"Reading inventory from {file_path}")
                with warnings.catch_warnings(record=True) as caught_warnings:
                    warnings.simplefilter("always")
                    inv = read_inventory(str(file_path))
                    for w in caught_warnings:
                        logger.warning(f"{filename}: {str(w.message)}")
                break # Found it, stop looking
            except Exception as e:
                logger.warning(f"Failed to read local inventory {file_path}: {e}")
                continue
    
    if not inv:
        logger.warning(f"No local inventory file found for {net}.{sta} in {inventory_path}")
        return None

    # Now, select the exact channel at the correct time
    try:
        channel_inv = inv.select(
            network=net,
            station=sta,
            location=loc,
            channel=cha,
            starttime=time0,
            endtime=time0
        )
        if len(channel_inv.networks) == 0:
            logger.warning(f"Found inventory for {net}.{sta}, but channel {loc}.{cha} not valid for {time0}")
            return None
        return channel_inv
    except Exception as e:
        logger.error(f"Error selecting channel from local inventory for {net}.{sta}: {e}")
        return None