import numpy as np
import pandas as pd
from obspy import Stream, UTCDateTime
import logging

logger = logging.getLogger(__name__)

# TO DO : Examine memory efficient spike calculation method
def _calculate_spikes(st: Stream, wn: int, sigma: int, method: str = 'fast'):
    """
    Calculates the total number of spikes across all traces in a Stream.

    Parameters
    ----------
    st : obspy.Stream
        The stream containing traces.
    wn : int
        Window size for rolling median/MAD.
    sigma : int or float
        Threshold multiplier for spike detection.
    method : str, optional
        Either 'fast' (high-memory, fast) or 'efficient' (low-memory, slower).

    Returns
    -------
    int
        Total number of spikes across all traces.
    """

    num_spikes_total = 0
    logger.debug(f"Using {method} spike calculation method.")

    for tr in st:
        data = tr.data.astype(np.float64)
        num_spike_trace = 0

        try:
            # Skip too-short traces
            if len(data) < wn * 2:
                continue

            # --- Engine 1: 'efficient' (Low-Memory, Slow but consistent) ---
            if method == 'efficient':
                # Ensure odd window size for symmetry
                wn_odd = wn + 1 if wn % 2 == 0 else wn
                half_window = wn_odd // 2

                s = pd.Series(data)

                # Rolling median and MAD with full valid window, NaN-tolerant
                x_median = (
                    s.rolling(window=wn_odd, center=True, min_periods=wn_odd)
                    .median()
                )

                def mad_func(x):
                    x = x[~np.isnan(x)]  # remove NaNs
                    if len(x) == 0:
                        return np.nan
                    med = np.median(x)
                    return np.median(np.abs(x - med))

                x_mad = (
                    s.rolling(window=wn_odd, center=True, min_periods=wn_odd)
                    .apply(mad_func, raw=True)
                )

                # Absolute difference
                difference = (s - x_median).abs()
                threshold = 1.4826 * sigma * x_mad + 1e-9

                # Restrict to valid central region (ignore edges)
                valid_slice = slice(half_window, len(s) - half_window)
                diff_valid = difference.iloc[valid_slice]
                thr_valid = threshold.iloc[valid_slice]

                # Compare (both NaN-safe)
                mask = (diff_valid > thr_valid) & (~diff_valid.isna()) & (~thr_valid.isna())
                num_spike_trace = int(mask.sum())

            # --- Engine 2: 'fast' (High-Memory, Fast) ---
            else:
                mad_array = lambda x: np.median(np.abs(x - np.median(x)), axis=1)
                diff_array = lambda x: np.median(x, axis=1)
                
                N = len(data)
                window_size = wn + 1
                start_index = int(window_size / 2)
                vert_idx_list = np.arange(0, N - wn, 1)
                
                if len(vert_idx_list) == 0:
                    continue
                    
                hori_idx_list = np.arange(window_size)
                A, B = np.meshgrid(hori_idx_list, vert_idx_list)
                idx_array = A + B
                x_array = data[idx_array]
                
                mad = mad_array(x_array)
                x_mean = diff_array(x_array)
                
                data_centered = data[start_index : start_index + len(vert_idx_list)]
                difference = np.abs(data_centered - x_mean)
                threshold = 1.4826 * sigma * mad + 1e-9
                
                outlier_idx = np.full(difference.shape, False)
                mask = threshold > 0
                outlier_idx[mask] = difference[mask] > threshold[mask]
                
                num_spike_trace = np.sum(outlier_idx)
        
        except MemoryError:
            logger.error(
                f"MemoryError processing spikes for {tr.id} with '{method}' method. "
                f"Trace length: {len(tr.data)}. Skipping spike calculation for this trace."
            )
            continue
        except Exception as e:
            logger.warning(
                f"Spike calculation failed for {tr.id}: {e}"
            )
            continue
            
        num_spikes_total += num_spike_trace
        
    return num_spikes_total

def _calculate_rms(st: Stream):
    """Calculates the average RMS of all traces in a Stream."""
    if not st:
        return 0.0
    
    rms_values = []
    for tr in st:
        if tr.stats.npts == 0 or len(tr.data) == 0:
            continue
        
        # Use float64 for precision and nanmean to handle gaps
        try:
            mean_square = np.nanmean(np.square(tr.data.astype(np.float64)- np.nanmean(tr.data.astype(np.float64))))
            if not np.isnan(mean_square) and mean_square >= 0:
                rms_values.append(np.sqrt(mean_square))
        except Exception:
            continue # Skip trace on numerical error

    if not rms_values:
        return 0.0
        
    return sum(rms_values) / len(rms_values)

def _calculate_percent_availability(st: Stream, day_start_time: UTCDateTime, day_end_time: UTCDateTime):
    """
    Calculates the percentage of data availability against a fixed daily window.
    """
    if not st:
        return 0.0
    
    # 1. The Denominator: The total window size (handles leap seconds)
    total_day_duration = day_end_time - day_start_time
    if total_day_duration <= 0:
        return 0.0

    try:
        # 2. The Numerator: The actual duration of data present in the stream
        stream_starttime = min(tr.stats.starttime for tr in st)
        stream_endtime = max(tr.stats.endtime for tr in st)
        
        # This is the total time span *covered* by the data
        stream_span = stream_endtime - stream_starttime
        
        if stream_span < 0: # Data is invalid
            return 0.0

        # Find all gaps *within* that span
        delta_gaps = 0
        for gap in st.get_gaps():
            if gap[6] > 0: # gap[6] is the duration
                delta_gaps += gap[6]
        
        # This is the actual amount of data recorded
        actual_data_duration = stream_span - delta_gaps
        
        # 3. The Final Formula
        percentage = 100 * (actual_data_duration / total_day_duration)
        
        # Cap at 100% in case of rounding or minor data overlaps beyond the day
        return min(100.0, round(percentage, 2))

    except Exception as e:
        stream_id = st[0].id if st else "empty stream" # type: ignore
        logger.warning(f"Could not calculate availability for {stream_id}: {e}")
        return 0.0

def _calculate_gaps_overlaps(st: Stream):
    """Counts the number of gaps and overlaps in a Stream."""
    try:
        result = st.get_gaps()
        gaps = 0
        overlaps = 0
        for r in result:
            if r[6] > 0:
                gaps += 1
            else:
                overlaps += 1
        return gaps, overlaps
    except Exception as e:
        stream_id = st[0].id if st else "empty stream" # type: ignore
        logger.warning(f"Could not calculate gaps/overlaps for {stream_id}: {e}")
        return 99999.0, 99999.0

def _calculate_stream_amplitude(st: Stream):
    """Finds the min and max amplitude across all traces in a Stream."""
    ampmax = -np.inf
    ampmin = np.inf
    valid_data_found = False
    
    for tr in st:
        data = tr.data
        if not isinstance(data, np.ndarray) or len(data) == 0 or np.all(np.isnan(data)):
            continue
            
        try:
            current_max = np.nanmax(data)
            current_min = np.nanmin(data)
            
            if current_max > ampmax:
                ampmax = current_max
            if current_min < ampmin:
                ampmin = current_min
            valid_data_found = True
        except Exception:
            continue

    if not valid_data_found:
        return np.nan, np.nan
    else:
        return ampmax, ampmin

def _calculate_ratioamp(ampmin, ampmax):
    """Calculates the ratio of max/min amplitude."""
    ratio = 0.0
    if np.isnan(ampmax) or np.isnan(ampmin):
        ratio = 0.0
    elif ampmax == 0 or ampmin == 0:
        ratio = 1.0
    elif ampmax > ampmin:
        ratio = ampmax / ampmin
    else:
        ratio = ampmin / ampmax
    
    return min(ratio, 99999.0)

def process_basic_metrics(data: Stream, day_start_time: UTCDateTime, day_end_time: UTCDateTime, spike_method: str = 'fast'):
    """
    Main function to calculate all basic metrics from a stream.
    Now calculates ratioamp internally.
    """
    st = data.copy()
    # st.detrend()
    
    rms = _calculate_rms(st)
    if rms > 99999:
        rms = 99999.0
    
    # 1. Get raw amplitude
    ampmax, ampmin = _calculate_stream_amplitude(st)
    
    # 2. Calculate ratioamp internally
    ampmax_abs = abs(ampmax)
    ampmin_abs = abs(ampmin)
    ratioamp = _calculate_ratioamp(ampmin_abs, ampmax_abs)
    
    # 3. Get other metrics
    psdata = _calculate_percent_availability(st, day_start_time, day_end_time)
    ngap, nover = _calculate_gaps_overlaps(st)
    num_spikes = _calculate_spikes(st, 80, 10, spike_method)
    
    # 4. Return all metrics
    final_metrics = {
        'rms': rms,
        # 'ampmax': ampmax,
        # 'ampmin': ampmin,
        'ratioamp': ratioamp,
        'psdata': psdata,
        'ngap': ngap,
        'nover': nover,
        'num_spikes': num_spikes
    }
    return final_metrics