import numpy as np
from obspy.clients.fdsn import Client
from obspy import Stream, Trace, Inventory, UTCDateTime
from typing import Optional, cast
import logging

logger = logging.getLogger(__name__)

def get_location_info(st: Stream):
    """Finds unique location codes in a stream."""
    location = [tr.stats.location for tr in st]
    tmp = np.array(location)
    return np.unique(tmp)