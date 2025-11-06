import logging
from typing import Optional
from obspy import Stream, UTCDateTime
from obspy.clients.filesystem.sds import Client as SDSClient

logger = logging.getLogger(__name__)

def get_waveforms(client: SDSClient, net: str, sta: str, loc: str, 
                    channel_prefixes: list, time0: UTCDateTime, 
                    time1: UTCDateTime, c: str) -> Optional[Stream]:
    """
    Attempts to read waveform data from an SDS archive using the ObsPy SDS client.
    Iterates through channel_prefixes to find the first matching data.
    """
    
    # Handle empty location code (ObsPy client expects "")
    loc_id = loc if loc else ""

    # Iterate through prefixes
    for prefix in channel_prefixes:
        cha = f"{prefix}{c}"
        
        try:
            # Let the client find, read, and trim the file
            st = client.get_waveforms(
                network=net,
                station=sta,
                location=loc_id,
                channel=cha,
                starttime=time0,
                endtime=time1
            )
            
            if st.count() > 0:
                st.merge(method=1) 
                logger.debug(f"Success: Loaded {net}.{sta}.{loc_id}.{cha} from SDS")
                return st

        except Exception as e:
            # This will happen if the file or directory doesn't exist
            logger.debug(f"No data found in SDS for {net}.{sta}.{loc_id}.{cha}: {e}")
            continue

    logger.debug(f"All SDS prefixes failed for {net}.{sta}.{loc_id}.*{c} on {time0.date}")
    return None