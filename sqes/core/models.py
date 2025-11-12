import math
import numpy as np
import logging

logger = logging.getLogger(__name__)

def get_models(periods, powers):
    """
    Calculates the NHNM (New High Noise Model) and NLNM (New Low Noise Model)
    values for a given set of periods.
    
    Args:
        periods (np.array): Array of period values.
        powers (list): List of power values (e.g., -190 to -90 dB).

    Returns:
        Tuple[np.array, np.array, list]:
            - NHNM (New High Noise Model values)
            - NLNM (New Low Noise Model values)
            - PERIODS_IDX (Indices of the original periods that are valid)
    """
    NHNM = []
    NLNM = []
    PERIODS_IDX = [] # The indices corresponding to periods within the defined models

    # --- NHNM Constants (Peterson, 1993) ---
    Ph = [0.10, 0.22, 0.32, 0.80, 3.80, 4.60, 6.30, 7.90, 15.40, 20.00, 354.80, 100000.00]
    Ah = [-108.73, -150.34, -122.31, -116.85, -108.48, -74.66, 0.66, -93.37, 73.54, -151.52, -206.66]
    Bh = [-17.23, -80.50, -23.87, 32.51, 18.08, -32.95, -127.18, -22.42, -162.98, 10.01, 31.63]
    
    # --- NLNM Constants (Peterson, 1993) ---
    Pl = [0.10, 0.17, 0.40, 0.80, 1.24, 2.40, 4.30, 5.00, 6.00, 10.00, 12.00, 15.60, 21.90, 
            31.60, 45.00, 70.00, 101.00, 154.00, 328.00, 600.00, 10000.00, 100000.00]
    Al = [-162.36, -166.7, -170.00, -166.40, -168.60, -159.98, -141.10, -71.36, -97.26, 
            -132.18, -205.27, -37.65, -114.37, -160.58, -187.50, -216.47, -185.00, -168.34, 
            -217.43, -258.28, -346.88]
    Bl = [5.64, 0.00, -8.30, 28.90, 52.48, 29.81, 0.00, -99.77, -66.49, -31.57, 36.16, 
            -104.33, -47.10, -16.28, 0.00, 15.70, 0.00, -7.61, 11.90, 26.60, 48.75]

    pInd = 0
    for period in periods:
        try:
            # Find the segment this period falls into
            highInd = [i for i, x in enumerate([period > Ph][0]) if x][-1]
            lowInd = [i for i, x in enumerate([period > Pl][0]) if x][-1]

            if highInd >= len(Ah) or lowInd >= len(Al):
                pInd += 1
                continue
            
        except IndexError:
            # Period is outside the defined model range (e.g., < 0.1s)
            pInd += 1
            continue

        # Calculate the noise model power (in dB) for this period
        nhnm = Ah[highInd] + Bh[highInd] * math.log10(period)
        nlnm = Al[lowInd] + Bl[lowInd] * math.log10(period)
        
        NHNM.append(nhnm)
        NLNM.append(nlnm)
        PERIODS_IDX.append(pInd)
        pInd += 1

    return np.array(NHNM), np.array(NLNM), PERIODS_IDX