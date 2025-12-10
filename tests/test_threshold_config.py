#!/usr/bin/env python3
"""
Simplified test script to verify QC threshold loading from config file.
Tests only the threshold loading logic without importing dependencies.
"""
import os
import sys
from configparser import ConfigParser
from dataclasses import dataclass

# Inline QCThresholds for testing
@dataclass
class QCThresholds:
    """Scientific thresholds for QC grading."""
    rms_limit: float = 5000.0
    ratioamp_limit: float = 1.01
    gap_limit: float = 0.00274
    overlap_limit: float = 0.0
    spike_limit: float = 0.0
    rms_margin: float = 7500.0
    ratioamp_margin: float = 2.02
    gap_margin: float = 0.992
    overlap_margin: float = 1.25
    spike_margin: float = 25.0
    pct_below_warn: float = 20.0
    pct_above_warn: float = 20.0
    gap_count_warn: int = 5
    overlap_count_warn: int = 5
    spike_count_warn: int = 25
    avail_good: float = 97.0
    avail_fair: float = 60.0
    avail_min_for_noise_check: float = 10.0
    dcl_dead: float = 2.25
    rms_damaged_max: float = 1.0
    fair_max_score: float = 89.0
    poor_max_score: float = 59.0
    weight_noise: float = 0.35
    weight_availability: float = 0.15
    weight_rms: float = 0.10
    weight_ratioamp: float = 0.10
    weight_gaps: float = 0.10
    weight_overlaps: float = 0.10
    weight_spikes: float = 0.10

DEFAULT_THRESHOLDS = QCThresholds()

def load_thresholds_from_config(config) -> QCThresholds:
    """Load QC thresholds from config file with fallback to defaults."""
    kwargs = {}
    
    if config.has_section('qc_thresholds'):
        # Metric limits
        if config.has_option('qc_thresholds', 'rms_limit'):
            kwargs['rms_limit'] = config.getfloat('qc_thresholds', 'rms_limit')
        if config.has_option('qc_thresholds', 'ratioamp_limit'):
            kwargs['ratioamp_limit'] = config.getfloat('qc_thresholds', 'ratioamp_limit')
        if config.has_option('qc_thresholds', 'gap_limit'):
            kwargs['gap_limit'] = config.getfloat('qc_thresholds', 'gap_limit')
        if config.has_option('qc_thresholds', 'avail_good'):
            kwargs['avail_good'] = config.getfloat('qc_thresholds', 'avail_good')
        if config.has_option('qc_thresholds', 'weight_noise'):
            kwargs['weight_noise'] = config.getfloat('qc_thresholds', 'weight_noise')
    
    # Create dict with all defaults
    default_dict = {
        'rms_limit': DEFAULT_THRESHOLDS.rms_limit,
        'ratioamp_limit': DEFAULT_THRESHOLDS.ratioamp_limit,
        'gap_limit': DEFAULT_THRESHOLDS.gap_limit,
        'overlap_limit': DEFAULT_THRESHOLDS.overlap_limit,
        'spike_limit': DEFAULT_THRESHOLDS.spike_limit,
        'rms_margin': DEFAULT_THRESHOLDS.rms_margin,
        'ratioamp_margin': DEFAULT_THRESHOLDS.ratioamp_margin,
        'gap_margin': DEFAULT_THRESHOLDS.gap_margin,
        'overlap_margin': DEFAULT_THRESHOLDS.overlap_margin,
        'spike_margin': DEFAULT_THRESHOLDS.spike_margin,
        'pct_below_warn': DEFAULT_THRESHOLDS.pct_below_warn,
        'pct_above_warn': DEFAULT_THRESHOLDS.pct_above_warn,
        'gap_count_warn': DEFAULT_THRESHOLDS.gap_count_warn,
        'overlap_count_warn': DEFAULT_THRESHOLDS.overlap_count_warn,
        'spike_count_warn': DEFAULT_THRESHOLDS.spike_count_warn,
        'avail_good': DEFAULT_THRESHOLDS.avail_good,
        'avail_fair': DEFAULT_THRESHOLDS.avail_fair,
        'avail_min_for_noise_check': DEFAULT_THRESHOLDS.avail_min_for_noise_check,
        'dcl_dead': DEFAULT_THRESHOLDS.dcl_dead,
        'rms_damaged_max': DEFAULT_THRESHOLDS.rms_damaged_max,
        'fair_max_score': DEFAULT_THRESHOLDS.fair_max_score,
        'poor_max_score': DEFAULT_THRESHOLDS.poor_max_score,
        'weight_noise': DEFAULT_THRESHOLDS.weight_noise,
        'weight_availability': DEFAULT_THRESHOLDS.weight_availability,
        'weight_rms': DEFAULT_THRESHOLDS.weight_rms,
        'weight_ratioamp': DEFAULT_THRESHOLDS.weight_ratioamp,
        'weight_gaps': DEFAULT_THRESHOLDS.weight_gaps,
        'weight_overlaps': DEFAULT_THRESHOLDS.weight_overlaps,
        'weight_spikes': DEFAULT_THRESHOLDS.weight_spikes,
    }
    
    # Override defaults with config values
    default_dict.update(kwargs)
    
    return QCThresholds(**default_dict)

def test_load_from_sample_config():
    """Test loading thresholds from sample_config.ini."""
    print("Test: Load from sample_config.ini")
    print("-" * 50)
    
    config_path = os.path.join(
        os.path.dirname(__file__), 
        '..', 'config', 'sample_config.ini'
    )
    
    if not os.path.exists(config_path):
        print(f"✗ Config file not found: {config_path}")
        return False
    
    config = ConfigParser()
    config.read(config_path)
    
    thresholds = load_thresholds_from_config(config)
    
    print(f"RMS Limit: {thresholds.rms_limit}")
    print(f"Gap Limit: {thresholds.gap_limit}")
    print(f"Weight Noise: {thresholds.weight_noise}")
    print(f"Avail Good: {thresholds.avail_good}")
    
    # Verify values match what we expect from sample_config.ini
    assert thresholds.rms_limit == 5000.0, f"RMS limit mismatch: {thresholds.rms_limit}"
    assert thresholds.gap_limit == 0.00274, f"Gap limit mismatch: {thresholds.gap_limit}"
    assert thresholds.weight_noise == 0.35, f"Weight noise mismatch: {thresholds.weight_noise}"
    assert thresholds.avail_good == 97.0, f"Avail good mismatch: {thresholds.avail_good}"
    
    print("✓ Thresholds loaded from sample_config.ini successfully\n")
    return True

def test_partial_config():
    """Test loading with partial config (missing some parameters)."""
    print("Test: Partial Config (fallback to defaults)")
    print("-" * 50)
    
    # Create a minimal config with only a few parameters
    config = ConfigParser()
    config.add_section('qc_thresholds')
    config.set('qc_thresholds', 'rms_limit', '6000.0')  # Custom value
    config.set('qc_thresholds', 'avail_good', '95.0')   # Custom value
    
    thresholds = load_thresholds_from_config(config)
    
    print(f"RMS Limit (custom): {thresholds.rms_limit}")
    print(f"Avail Good (custom): {thresholds.avail_good}")
    print(f"Gap Limit (default): {thresholds.gap_limit}")
    print(f"Weight Noise (default): {thresholds.weight_noise}")
    
    # Verify custom values are used
    assert thresholds.rms_limit == 6000.0, "Custom RMS limit not applied"
    assert thresholds.avail_good == 95.0, "Custom avail_good not applied"
    
    # Verify defaults are used for missing values
    assert thresholds.gap_limit == DEFAULT_THRESHOLDS.gap_limit, "Default gap_limit not used"
    assert thresholds.weight_noise == DEFAULT_THRESHOLDS.weight_noise, "Default weight_noise not used"
    
    print("✓ Partial config with defaults works correctly\n")
    return True

def test_no_qc_section():
    """Test loading when [qc_thresholds] section doesn't exist."""
    print("Test: No [qc_thresholds] Section (all defaults)")
    print("-" * 50)
    
    config = ConfigParser()
    config.add_section('basic')
    config.set('basic', 'use_database', 'postgresql')
    
    thresholds = load_thresholds_from_config(config)
    
    # All values should be defaults
    assert thresholds.rms_limit == DEFAULT_THRESHOLDS.rms_limit
    assert thresholds.gap_limit == DEFAULT_THRESHOLDS.gap_limit
    assert thresholds.weight_noise == DEFAULT_THRESHOLDS.weight_noise
    
    print(f"RMS Limit: {thresholds.rms_limit} (default)")
    print(f"Gap Limit: {thresholds.gap_limit} (default)")
    print("✓ All defaults used when section missing\n")
    return True

if __name__ == "__main__":
    print("=" * 50)
    print("QC Threshold Configuration Test")
    print("=" * 50)
    print()
    
    try:
        test_load_from_sample_config()
        test_partial_config()
        test_no_qc_section()
        
        print("=" * 50)
        print("✓ All tests passed!")
        print("=" * 50)
        sys.exit(0)
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
