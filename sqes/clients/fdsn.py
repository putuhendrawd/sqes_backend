import logging
from typing import Optional, cast
from obspy import Stream, Trace, Inventory, UTCDateTime
from obspy.clients.fdsn import Client as FDSNClient
from sqes.core import utils

logger = logging.getLogger(__name__)

def get_waveforms(client: FDSNClient, net: str, sta: str, loc: str, 
                       channel_prefixes: list, time0: UTCDateTime, 
                       time1: UTCDateTime, c: str) -> Optional[Stream]:
    """
    Attempts to download waveform data from an FDSN client, 
    iterating through channel prefixes.
    Returns the Stream object if successful, else None.
    """
    for channel_prefix in channel_prefixes:
        channel_code = f"{channel_prefix}{c}"
        try:
            st = client.get_waveforms(net, sta, loc, channel_code, time0, time1)
            
            if st and st.count() > 0:
                if st.count() > 1:
                    # Use the helper from utils.py
                    loc_ = utils.get_location_info(st)
                    st = st.select(location=loc_[0])
                
                first_trace = cast(Trace, st[0])
                logger.debug(f"Success: Got waveform {first_trace.id} from FDSN")
                return st
        
        except Exception:
            logger.debug(f"No data for {net}.{sta}.{loc}.{channel_code} from FDSN")
            continue
    
    logger.debug(f"All FDSN prefixes failed for {net}.{sta}.{loc}.*{c}")
    return None

def get_inventory(client: FDSNClient, net: str, sta: str, 
                       loc: str, cha: str, time0: UTCDateTime) -> Optional[Inventory]:
    """
    Attempts to download inventory for a specific, known channel from FDSN.
    """
    try:
        inv = client.get_stations(
            network=net, 
            station=sta, 
            location=loc, 
            channel=cha, 
            level="response",
            starttime=time0 # Use time0 to get the correct epoch
        )
        return inv
    except Exception as e:
        logger.warning(f"Could not get inventory for {net}.{sta}.{loc}.{cha}: {e}")
        return None