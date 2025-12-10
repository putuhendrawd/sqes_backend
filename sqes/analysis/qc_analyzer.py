# sqes/analysis/qc_analyzer.py
import time
import logging
import numpy as np
from typing import Optional, List, Tuple
from ..services.repository import QCRepository
from .models import QCThresholds, DEFAULT_THRESHOLDS

logger = logging.getLogger(__name__)


def calculate_metric_grade(value: float, threshold: float, margin: float) -> float:
    """
    Calculate a grade (0-100) for a QC metric based on linear degradation.
    
    Args:
        value: The measured parameter value
        threshold: The acceptable limit (90th percentile or scientific guideline)
        margin: The margin for degradation before reaching 0 score
        
    Returns:
        float: Grade from 0.0 to 100.0
        
    Formula:
        grade = 100 - (GRADE_SLOPE * (value - threshold) / margin)
        
    Behavior:
        - At threshold: 100%
        - At threshold + margin: 85%
        - At threshold + (100/GRADE_SLOPE)*margin: 0%
    """
    GRADE_SLOPE = 15.0
    grade = 100.0 - (GRADE_SLOPE * (value - threshold) / margin)
    return max(0.0, min(100.0, grade))


def validate_qc_metrics(
    rms: float, ratioamp: float, avail: float, 
    ngap1: int, nover: int, num_spikes: int,
    pct_above: float, pct_below: float, dcl: float, dcg: float,
    station_code: str, component: str
) -> List[str]:
    """
    Validate QC metric ranges to catch data corruption or anomalies.
    
    Args:
        All QC metrics and station/component identifiers
        
    Returns:
        List of validation error messages (empty if all valid)
    """
    issues = []
    
    if rms < 0:
        issues.append(f"{station_code}.{component}: Invalid RMS={rms} (must be >= 0)")
    if ratioamp < 0:
        issues.append(f"{station_code}.{component}: Invalid amplitude ratio={ratioamp} (must be >= 0)")
    if not (0 <= avail <= 100):
        issues.append(f"{station_code}.{component}: Invalid availability={avail}% (must be 0-100)")
    if ngap1 < 0:
        issues.append(f"{station_code}.{component}: Invalid gap count={ngap1} (must be >= 0)")
    if nover < 0:
        issues.append(f"{station_code}.{component}: Invalid overlap count={nover} (must be >= 0)")
    if num_spikes < 0:
        issues.append(f"{station_code}.{component}: Invalid spike count={num_spikes} (must be >= 0)")
    if not (0 <= pct_above <= 100):
        issues.append(f"{station_code}.{component}: Invalid pct_above={pct_above}% (must be 0-100)")
    if not (0 <= pct_below <= 100):
        issues.append(f"{station_code}.{component}: Invalid pct_below={pct_below}% (must be 0-100)")
    if pct_above + pct_below > 100:
        issues.append(
            f"{station_code}.{component}: Invalid PSD percentages: "
            f"above={pct_above}% + below={pct_below}% = {pct_above+pct_below}% > 100%"
        )
    if dcg not in (0, 1):
        issues.append(f"{station_code}.{component}: Invalid DCG flag={dcg} (must be 0 or 1)")
    
    return issues


def determine_warning(
    component: str, avail: float, pct_below: float, pct_above: float,
    ngap1: int, nover: int, num_spikes: int, 
    thresholds: QCThresholds = DEFAULT_THRESHOLDS
) -> List[str]:
    """
    Determine all warning messages that match the QC criteria.
    
    All conditions are checked and all matching warnings are returned.
    
    Warning types:
    - Metadata issues (pct_below > threshold)
    - Gap issues
    - Overlap issues
    - Noise issues
    - Spike issues
    - Availability issues
    
    Args:
        component: Component identifier (E, N, Z)
        All relevant QC metrics
        thresholds: QCThresholds configuration
        
    Returns:
        List of warning message strings (empty list if no warnings)
    """
    warnings = []
    
    if pct_below > thresholds.pct_below_warn:
        warnings.append(f"Cek metadata komponen {component}")
    if ngap1 > thresholds.gap_count_warn:
        warnings.append(f"Terlalu banyak gap pada komponen {component}")
    if nover > thresholds.overlap_count_warn:
        warnings.append(f"Terlalu banyak overlap pada komponen {component}")
    if pct_above > thresholds.pct_above_warn and avail >= thresholds.avail_min_for_noise_check:
        warnings.append(f"Noise tinggi di komponen {component}")
    if num_spikes > thresholds.spike_count_warn:
        warnings.append(f"Spike berlebihan pada komponen {component}")
    if avail < thresholds.avail_good and avail >= thresholds.avail_fair:
        warnings.append(f"Availability rendah pada komponen {component}")
    if avail < thresholds.avail_fair and avail > 0:
        warnings.append(f"Availability sangat rendah pada komponen {component}")
    
    return warnings


def check_qc(score: float) -> str:
    """
    Assign a quality classification string based on the final score.
    
    Args:
        score: Quality score (0-100)
        
    Returns:
        Quality classification: 'Baik', 'Cukup Baik', 'Buruk', or 'Mati'
    """
    if score >= 90.0:
        return 'Baik'
    elif score >= 60.0:
        return 'Cukup Baik'
    elif score == 0.0:
        return 'Mati'
    else:
        return 'Buruk'

def aggregate_station_score(component_scores, method='p25'):
    """
    Aggregate component scores to station score.
    
    Methods:
        'p25': 25th percentile (current, conservative)
        'mean': Arithmetic mean (optimistic)
        'hmean': Harmonic mean (penalizes low values more)
        'gmean': Geometric mean (balanced)
        'min': Minimum component (most conservative)
        'median' : Median component (middle value)
    """
    if method == 'p25':
        return np.percentile(component_scores, 25)
    elif method == 'mean':
        return np.mean(component_scores)
    elif method == 'hmean':
        from scipy.stats import hmean
        return hmean(component_scores)
    elif method == 'gmean':
        from scipy.stats import gmean
        return gmean(component_scores)
    elif method == 'min':
        return np.min(component_scores)
    elif method == 'median':
        return np.median(component_scores)
    else:
        raise ValueError(f"Unknown method: {method}")

def run_qc_analysis(
    repo: QCRepository, 
    db_type: str, 
    tanggal: str, 
    station_code: str,
    thresholds: QCThresholds = DEFAULT_THRESHOLDS
) -> None:
    """
    Main QC Analysis function.
    
    Fetches all component data for a station, validates metrics,
    computes individual component scores, aggregates to station score,
    and stores results with quality classification and warnings.
    
    Args:
        repo: Database repository for QC operations
        db_type: Database type ('mysql' or 'postgresql')
        tanggal: Date string (YYYYMMDD)
        station_code: Station code to analyze
        thresholds: QCThresholds configuration (optional, uses defaults if not provided)
    """
    # 1. Flush any existing analysis data for this station/day
    try:
        repo.flush_analysis_result(tanggal, station_code)
        logger.debug(f"Ready to fill analysis for {station_code} on {tanggal}")
    except Exception as e:
        logger.error(f"Failed to flush analysis for {station_code}: {e}")
        return  # Cannot proceed

    # 2. Get station info (e.g., 'tipe' or 'network_group')
    station_info = repo.get_station_info(station_code)
    
    if not station_info:
        logger.warning(f"<{station_code}> No station info found in database. Skipping analysis.")
        return

    # This loop will only run once, but it's an easy way to unpack the data
    for sta in station_info:
        network = sta[0]
        kode = sta[1]
        tipe = sta[3]

        # 3. Get all QC details for this station and day
        dataqc = repo.get_qc_details_for_station(tanggal, kode)
        
        if not dataqc:
            logger.warning(f"<{tipe}> {kode} no QC detail data exist, logging as 'Mati'")
            repo.insert_qc_analysis_result(kode, tanggal, '0', 'Mati', tipe, ['Tidak ada data'])
            continue
        
        percqc_list = []
        ket = []
        
        # 4. Loop through each component (E, N, Z)
        for qc_row in dataqc:
            # 5. Map columns based on DB type
            try:
                if db_type == 'mysql':
                    komp = qc_row[4]
                    rms = float(qc_row[5])
                    ratioamp = float(qc_row[6])
                    avail = float(qc_row[7])
                    ngap1 = int(qc_row[8])
                    nover = int(qc_row[9])
                    num_spikes = int(qc_row[10])
                    pct_above = float(qc_row[11])
                    pct_below = float(qc_row[12])
                    dcl = float(qc_row[13])
                    dcg = float(qc_row[14])
                elif db_type == 'postgresql':
                    komp = qc_row[3]
                    rms = float(qc_row[4])
                    ratioamp = float(qc_row[5])
                    avail = float(qc_row[6])
                    ngap1 = int(qc_row[7])
                    nover = int(qc_row[8])
                    num_spikes = int(qc_row[9])
                    pct_below = float(qc_row[10])
                    pct_above = float(qc_row[11])
                    dcl = float(qc_row[12])
                    dcg = float(qc_row[13])  # This is the binary 0 or 1
                else:
                    logger.error(f"Unknown database type: {db_type}")
                    continue
            except (ValueError, TypeError, IndexError) as e:
                logger.error(f"Error parsing QC row for {kode}: {e}. Row: {qc_row}")
                continue
            
            # 6. Validate metrics
            validation_issues = validate_qc_metrics(
                rms, ratioamp, avail, ngap1, nover, num_spikes,
                pct_above, pct_below, dcl, dcg, kode, komp
            )
            
            if validation_issues:
                for issue in validation_issues:
                    logger.warning(issue)
                # Continue with analysis even with validation warnings
            
            # 7. Perform grading logic for each metric
            
            # RMS grading
            if rms > thresholds.rms_damaged_max:
                rms_grade = calculate_metric_grade(abs(rms), thresholds.rms_limit, thresholds.rms_margin)
            else:
                rms_grade = 0.0  # Bad sensor
            
            # Availability adjustment
            if avail >= 100.0:
                ngap1 = 0
                avail = 100.0
            
            # Other metric grading
            ratioamp_grade = calculate_metric_grade(ratioamp, thresholds.ratioamp_limit, thresholds.ratioamp_margin)
            ngap_grade = calculate_metric_grade(ngap1, thresholds.gap_limit, thresholds.gap_margin)
            nover_grade = calculate_metric_grade(nover, thresholds.overlap_limit, thresholds.overlap_margin)
            num_spikes_grade = calculate_metric_grade(num_spikes, thresholds.spike_limit, thresholds.spike_margin)
            
            # Noise percentage (inside NHNM/NLNM bounds)
            pct_noise = 100.0 - pct_above - pct_below
            
            # 8. Calculate final weighted score for this component
            if avail <= 0.0:
                # Dead component
                botqc = 0.0
                ket.append(f'Komponen {komp} Mati')
            elif dcg == 1 or dcl <= thresholds.dcl_dead:
                # Unresponsive to vibration (QuARG guideline)
                botqc = 1.0
                ket.append(f'Komponen {komp} tidak merespon getaran')
            elif rms < thresholds.rms_damaged_max and rms > 0:
                # Damaged sensor
                botqc = 1.0
                ket.append(f'Komponen {komp} Rusak')
            else:
                # Normal weighted calculation
                botqc = (
                    thresholds.weight_noise * pct_noise +
                    thresholds.weight_availability * avail +
                    thresholds.weight_rms * rms_grade +
                    thresholds.weight_ratioamp * ratioamp_grade +
                    thresholds.weight_gaps * ngap_grade +
                    thresholds.weight_overlaps * nover_grade +
                    thresholds.weight_spikes * num_spikes_grade
                )
                
                # Determine all warning messages
                warnings = determine_warning(
                    komp, avail, pct_below, pct_above, 
                    ngap1, nover, num_spikes, thresholds
                )
                
                if warnings:
                    ket.extend(warnings)
                    
                    # Apply availability-based score capping
                    if avail < thresholds.avail_good and avail >= thresholds.avail_fair:
                        botqc = min(botqc, thresholds.fair_max_score)
                    elif avail < thresholds.avail_fair and avail > 0:
                        botqc = min(botqc, thresholds.poor_max_score)
            
            # Log detailed metrics for debugging
            logger.debug(
                f"{kode}.{komp}: "
                f"RMS={rms:.1f}({rms_grade:.1f}%), "
                f"Ratio={ratioamp:.2f}({ratioamp_grade:.1f}%), "
                f"Avail={avail:.1f}%, "
                f"Noise={pct_noise:.1f}%, "
                f"Gaps={ngap1}({ngap_grade:.1f}%), "
                f"Overlaps={nover}({nover_grade:.1f}%), "
                f"Spikes={num_spikes}({num_spikes_grade:.1f}%), "
                f"DCL={dcl:.2f}, DCG={dcg} -> "
                f"Score={botqc:.2f}"
            )
            
            # Append component score to list
            percqc_list.append(botqc)
        
        # 9. Aggregate component scores to station score
        if not percqc_list:
            score = 0.0
        else:
            # Use 25th percentile (conservative, reflects worst-performing component)
            # If any component is unresponsive/damaged (score=1.0), cap at Poor category
            if 1.0 in percqc_list:
                score = min(aggregate_station_score(percqc_list, 'p25'), thresholds.poor_max_score)
            else:
                score = aggregate_station_score(percqc_list, 'p25')
        
        # 10. Classify quality and store results
        kualitas = check_qc(score)
        
        repo.insert_qc_analysis_result(
            kode,
            tanggal,
            str(round(float(score), 2)),
            kualitas,
            tipe,
            ket
        )
        
        logger.info(
            f"{network}.{kode} ({tipe}) QC ANALYSIS FINISH "
            f"(Score: {score:.2f}, Quality: {kualitas}, Components: {len(percqc_list)})"
        )
        time.sleep(0.5)

    time.sleep(0.5)