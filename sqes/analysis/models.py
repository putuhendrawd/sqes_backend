# sqes/analysis/models.py
"""
Data models for QC analysis.

This module contains dataclasses and models used in quality control analysis,
separating data structures from analysis logic.
"""
from dataclasses import dataclass


@dataclass
class QCThresholds:
    """
    Scientific thresholds for QC grading based on Ringler et al. (2015) 
    90th percentile standards and QuARG guidelines.
    """
    # Metric limits (acceptable values)
    rms_limit: float = 5000.0
    ratioamp_limit: float = 1.01
    gap_limit: float = 0.00274  # QuARG
    overlap_limit: float = 0.0
    spike_limit: float = 0.0
    
    # Margins (degradation allowance before reaching 0 score)
    rms_margin: float = 7500.0
    ratioamp_margin: float = 2.02
    gap_margin: float = 0.992  # QuARG
    overlap_margin: float = 1.25
    spike_margin: float = 25.0
    
    # Warning thresholds
    pct_below_warn: float = 20.0
    pct_above_warn: float = 20.0
    gap_count_warn: int = 5
    overlap_count_warn: int = 5
    spike_count_warn: int = 25
    avail_good: float = 97.0
    avail_fair: float = 60.0
    avail_min_for_noise_check: float = 10.0
    
    # Dead channel thresholds (QuARG)
    dcl_dead: float = 2.25
    rms_damaged_max: float = 1.0
    
    # Score caps
    fair_max_score: float = 89.0
    poor_max_score: float = 59.0
    
    # Weights (must sum to 1.0)
    weight_noise: float = 0.35
    weight_availability: float = 0.15
    weight_rms: float = 0.10
    weight_ratioamp: float = 0.10
    weight_gaps: float = 0.10
    weight_overlaps: float = 0.10
    weight_spikes: float = 0.10


DEFAULT_THRESHOLDS = QCThresholds()
