import numpy as np
from numpy import polyfit
from obspy import Stream, Trace, Inventory
from obspy.signal import PPSD
from typing import Optional, cast
import logging

from obspy.imaging.cm import pqlx
from sqes.core import models

logger = logging.getLogger(__name__)

# --- Private Helper: PPSD Object Creation ---
def _create_ppsd_object(sig: Stream, inventory: Optional[Inventory] = None, npz_output_path: str = ''):
    """
    (Internal) Creates the PPSD object from a stream.
    This was formerly 'prosess_psd'.
    """
    NPZFNAME = '_{}.npz'
    data = sig.copy()
    
    if inventory is None:
        logger.warning('PPSD object creation skipped, no inventory provided.')
        return None
    if data.count() == 0:
        logger.warning('No data in stream for PPSD.')
        return None
        
    data.merge()
    
    _trace = cast(Trace, data[0])
    sampling_rate = _trace.stats.sampling_rate

    if sampling_rate == 0:
        logger.warning(f"Cannot process PPSD for {_trace.id}: sampling rate is 0.")
        return None
        
    min_samples = 3600 * sampling_rate
    if _trace.stats.npts <= min_samples:
        logger.warning(f"Not enough data for PPSD ({_trace.stats.npts} samples, need > {min_samples}) for {_trace.id}")
        return None
        
    ppsds_object = None
    _id = ''
    try:
        for tr in data:
            _id = tr.id
            ppsds_object = PPSD(tr.stats, inventory)
            ppsds_object.add(tr)
            
        if npz_output_path and ppsds_object:
            fname_out = npz_output_path + NPZFNAME.format(_id)
            logger.debug(f"Saving PPSD to {fname_out}")
            ppsds_object.save_npz(fname_out)
        
        return ppsds_object
        
    except Exception as e:
        logger.error(f"!! Error during PPSD object creation for {_id}: {e}")
        return None

# --- Private Helper: Calculation Functions ---
def _dead_channel_gsn(psd, model, t, t0=4.0, t1=8.0):
    mask = (t > t0) & (t < t1)
    psd_slice = psd[mask]
    model_slice = model[mask]
    if len(psd_slice) == 0: return 0.0
    gsn_deviation = np.mean(model_slice - psd_slice)

    return 1 if gsn_deviation > 5.0 else 0
    # return gsn_deviation

def _percentage_outside_model(psd, AHNM, ALNM):
    percH, percL, total_len = 0, 0, len(psd)
    if total_len == 0: return 0.0, 0.0
    for i in range(total_len):
        if psd[i] > AHNM[i]: percH += 1
        if psd[i] < ALNM[i]: percL += 1
    return round(float(percH * 100 / total_len), 2), round(float(percL * 100 / total_len), 2)

def _percentage_inside_model_by_period(psd, LNM, HNM, t, t0, t1):
    percH, mask = 0, (t > t0) & (t < t1)
    psd_slice, LNM_slice, HNM_slice = psd[mask], LNM[mask], HNM[mask]
    total_len = len(psd_slice)
    if total_len == 0: return 0.0
    for i in range(total_len):
        if (psd_slice[i] <= HNM_slice[i]) and (psd_slice[i] >= LNM_slice[i]):
            percH += 1
    return round(float(percH * 100 / total_len), 2)

def _dead_channel_lin(psd, t, fs):
    """
    Calculates linear dead channel metric based on the algorithm.
    This is the Root Mean Square Error (RMSE) of the linear fit.
    """
    # Safety check for sampling rate
    if fs <= 0:
        logger.warning("Sampling rate is <= 0, cannot calculate DCL metric.")
        return 0.0

    # Step 2: Trim the period range
    t0 = 4.0 / fs
    t1 = 100.0
    mask = (t > t0) & (t < t1)
    psd_slice = psd[mask]
    tn = t[mask]
    
    # Need at least 2 points to fit a line
    if len(psd_slice) < 2: 
        logger.debug(f"Not enough data points ({len(psd_slice)}) for DCL fit.")
        return 0.0
    
    # Step 3: Fit the line to [PSD mean vs. log(period)]
    tn_log = np.log10(tn)
    slope, intercept = polyfit(tn_log, psd_slice, 1)
    psdfit = slope * tn_log + intercept
    
    # Step 4: Calculate the standard deviation of the residuals (RMSE)
    # value = sqrt [ average( (BestFitLine - PSDmean)^2 ) ]
    value = np.sqrt(np.mean((psdfit - psd_slice)**2))
    
    return value

# --- NEW Main Public Function ---
def process_ppsd_metrics(sig: Stream, inventory, plot_filename: str, npz_output_path: str):
    """
    Calculates all PPSD metrics from a Stream and Inventory.
    
    This function creates the PPSD, plots it, and calculates all
    dead-channel and noise-model metrics.
    
    Returns:
        A dictionary of final metrics, or None if processing fails.
    """

    try:
        # 0. Validate Inputs
        _trace = cast(Trace, sig[0])

        # 1. Create the PPSD object
        ppsds = _create_ppsd_object(sig, inventory, npz_output_path)
        
        # 2. Safety Check (NEW)
        if not ppsds or not hasattr(ppsds, '_times_processed') or not ppsds._times_processed:
            raise ValueError(f"PPSD object for {_trace.id} is invalid or has no data.")
        
        # 2. Plot the PPSD
        if plot_filename:
            ppsds.plot(filename=plot_filename, cmap=pqlx, show=False, period_lim=(0.05, 100))
        
        fs = _trace.stats.sampling_rate
        
        # 3. Get Percentile Data
        period, psd1 = ppsds.get_percentile() # type: ignore
        ind = period <= 100
        period = period[ind]
        psd1 = psd1[ind]
        
        powers = sorted(range(-190, -90 + 1), reverse=True)
        NHNM, NLNM, PInd = models.get_models(period, powers)
        
        # Filter psd/period by valid model indices
        period = period[PInd]
        psd1 = psd1[PInd]
        
        if len(period) == 0:
            logger.warning(f"{_trace.id} No valid period data after model filtering")
            return None
        
        # 4. Calculate Percentile Metrics
        dcg = _dead_channel_gsn(psd1, NLNM, period)
        pctH, pctL = _percentage_outside_model(psd1, NHNM, NLNM)
        
        long_period = _percentage_inside_model_by_period(psd1, NLNM, NHNM, period, 20, 900)
        microseism = _percentage_inside_model_by_period(psd1, NLNM, NHNM, period, 2, 25)
        short_period = _percentage_inside_model_by_period(psd1, NLNM, NHNM, period, 0.1, 1)
        
        # 5. Get Mean Data and Calculate DCL
        period_mean, psd_mean = ppsds.get_mean() # type: ignore
        ind_mean = period_mean <= 100
        period_mean = period_mean[ind_mean]
        psd_mean = psd_mean[ind_mean]
        
        dcl = _dead_channel_lin(psd_mean, period_mean, fs)

        # 6. Assemble and return the metrics dictionary
        final_metrics = {
            'pctH': str(pctH),
            'pctL': str(pctL),
            'dcl': str(round(float(dcl), 2)),
            'dcg': str(round(dcg, 2)),
            'long_period': str(long_period),
            'microseism': str(microseism),
            'short_period': str(short_period)
        }
        return final_metrics
        
    except Exception as e:
        stream_id = _trace.id if sig else "empty stream"
        logger.error(f"!! Final PPSD metric calculation failed for {stream_id}: {e}")
        return None