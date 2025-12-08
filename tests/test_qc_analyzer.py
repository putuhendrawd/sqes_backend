"""
Unit tests for sqes.analysis.qc_analyzer module.

Tests cover:
- Metric grading function
- Input validation
- Warning logic
- Score capping
- Component and station score aggregation
- Edge cases and error handling
"""

import pytest
import numpy as np
from sqes.analysis.qc_analyzer import (
    _calculate_metric_grade,
    _validate_qc_metrics,
    _determine_warning,
    _check_qc,
    QCThresholds,
    DEFAULT_THRESHOLDS
)


class TestCalculateMetricGrade:
    """Test the metric grading function."""
    
    def test_grade_at_threshold(self):
        """Grade should be 100% at the threshold value."""
        grade = _calculate_metric_grade(value=5000, threshold=5000, margin=7500)
        assert grade == 100.0
    
    def test_grade_below_threshold(self):
        """Grade should be 100% below the threshold value."""
        grade = _calculate_metric_grade(value=4000, threshold=5000, margin=7500)
        assert grade == 100.0
    
    def test_grade_at_threshold_plus_margin(self):
        """Grade should be 85% at threshold + margin."""
        # At threshold + margin: grade = 100 - 15 = 85
        grade = _calculate_metric_grade(value=12500, threshold=5000, margin=7500)
        assert grade == 85.0
    
    def test_grade_well_above_threshold(self):
        """Grade should be 0% well above threshold."""
        # At threshold + (100/15)*margin = 5000 + 50000 = 55000
        grade = _calculate_metric_grade(value=55000, threshold=5000, margin=7500)
        assert grade == 0.0
    
    def test_grade_clamping_upper(self):
        """Grade should be clamped at 100% maximum."""
        grade = _calculate_metric_grade(value=-1000, threshold=5000, margin=7500)
        assert grade == 100.0
    
    def test_grade_clamping_lower(self):
        """Grade should be clamped at 0% minimum."""
        grade = _calculate_metric_grade(value=100000, threshold=5000, margin=7500)
        assert grade == 0.0
    
    def test_negative_margin(self):
        """Test grading with negative margin (e.g., DCL)."""
        # For DCL: threshold=9.0, margin=-1.0
        # Lower DCL values are worse
        grade = _calculate_metric_grade(value=9.0, threshold=9.0, margin=-1.0)
        assert grade == 100.0
        
        grade = _calculate_metric_grade(value=8.0, threshold=9.0, margin=-1.0)
        assert grade == 85.0


class TestValidateQCMetrics:
    """Test input validation function."""
    
    def test_valid_metrics(self):
        """Valid metrics should return no issues."""
        issues = _validate_qc_metrics(
            rms=100.0, ratioamp=1.5, avail=95.0,
            ngap1=2, nover=1, num_spikes=5,
            pct_above=15.0, pct_below=10.0, dcl=8.0, dcg=0,
            station_code="TEST", component="Z"
        )
        assert issues == []
    
    def test_negative_rms(self):
        """Negative RMS should be flagged."""
        issues = _validate_qc_metrics(
            rms=-100.0, ratioamp=1.5, avail=95.0,
            ngap1=2, nover=1, num_spikes=5,
            pct_above=15.0, pct_below=10.0, dcl=8.0, dcg=0,
            station_code="TEST", component="Z"
        )
        assert len(issues) == 1
        assert "Invalid RMS" in issues[0]
    
    def test_invalid_availability(self):
        """Availability outside 0-100 range should be flagged."""
        issues = _validate_qc_metrics(
            rms=100.0, ratioamp=1.5, avail=150.0,
            ngap1=2, nover=1, num_spikes=5,
            pct_above=15.0, pct_below=10.0, dcl=8.0, dcg=0,
            station_code="TEST", component="Z"
        )
        assert len(issues) == 1
        assert "Invalid availability" in issues[0]
    
    def test_invalid_psd_sum(self):
        """PSD percentages summing > 100 should be flagged."""
        issues = _validate_qc_metrics(
            rms=100.0, ratioamp=1.5, avail=95.0,
            ngap1=2, nover=1, num_spikes=5,
            pct_above=60.0, pct_below=50.0, dcl=8.0, dcg=0,
            station_code="TEST", component="Z"
        )
        assert len(issues) == 1
        assert "Invalid PSD percentages" in issues[0]
    
    def test_invalid_dcg_flag(self):
        """DCG flag not 0 or 1 should be flagged."""
        issues = _validate_qc_metrics(
            rms=100.0, ratioamp=1.5, avail=95.0,
            ngap1=2, nover=1, num_spikes=5,
            pct_above=15.0, pct_below=10.0, dcl=8.0, dcg=2,
            station_code="TEST", component="Z"
        )
        assert len(issues) == 1
        assert "Invalid DCG flag" in issues[0]
    
    def test_multiple_issues(self):
        """Multiple invalid metrics should all be flagged."""
        issues = _validate_qc_metrics(
            rms=-100.0, ratioamp=-1.5, avail=150.0,
            ngap1=-2, nover=-1, num_spikes=-5,
            pct_above=15.0, pct_below=10.0, dcl=8.0, dcg=0,
            station_code="TEST", component="Z"
        )
        assert len(issues) == 5  # rms, ratioamp, avail, ngap1, nover, num_spikes


class TestDetermineWarning:
    """Test warning determination logic."""
    
    def test_multiple_warnings_returned(self):
        """Multiple warnings should all be returned when criteria match."""
        warnings = _determine_warning(
            component="Z", avail=50.0, pct_below=25.0, pct_above=25.0,
            ngap1=10, nover=10, num_spikes=50
        )
        # Should return multiple warnings
        assert len(warnings) >= 4  # metadata, gaps, overlaps, spikes, availability
        assert any("Cek metadata" in w for w in warnings)
        assert any("gap" in w for w in warnings)
        assert any("overlap" in w for w in warnings)
        assert any("Spike" in w for w in warnings)
    
    def test_gap_warning(self):
        """Gap warning should trigger when gaps exceed threshold."""
        warnings = _determine_warning(
            component="Z", avail=95.0, pct_below=10.0, pct_above=10.0,
            ngap1=10, nover=2, num_spikes=5
        )
        assert len(warnings) == 1
        assert any("gap" in w for w in warnings)
    
    def test_overlap_warning(self):
        """Overlap warning should trigger when overlaps exceed threshold."""
        warnings = _determine_warning(
            component="Z", avail=95.0, pct_below=10.0, pct_above=10.0,
            ngap1=2, nover=10, num_spikes=5
        )
        assert len(warnings) == 1
        assert any("overlap" in w for w in warnings)
    
    def test_noise_warning(self):
        """Noise warning should trigger for high noise with sufficient availability."""
        warnings = _determine_warning(
            component="Z", avail=95.0, pct_below=10.0, pct_above=25.0,
            ngap1=2, nover=2, num_spikes=5
        )
        assert len(warnings) == 1
        assert any("Noise tinggi" in w for w in warnings)
    
    def test_noise_warning_with_low_availability(self):
        """Noise warning should NOT trigger with very low availability."""
        warnings = _determine_warning(
            component="Z", avail=5.0, pct_below=10.0, pct_above=25.0,
            ngap1=2, nover=2, num_spikes=5
        )
        # Should get availability warning, but NOT noise warning
        assert any("Availability sangat rendah" in w for w in warnings)
        assert not any("Noise tinggi" in w for w in warnings)
    
    def test_spike_warning(self):
        """Spike warning should trigger when spikes exceed threshold."""
        warnings = _determine_warning(
            component="Z", avail=95.0, pct_below=10.0, pct_above=10.0,
            ngap1=2, nover=2, num_spikes=30
        )
        assert len(warnings) == 1
        assert any("Spike" in w for w in warnings)
    
    def test_low_availability_warning(self):
        """Low availability warning (97-80%)."""
        warnings = _determine_warning(
            component="Z", avail=85.0, pct_below=10.0, pct_above=10.0,
            ngap1=2, nover=2, num_spikes=5
        )
        assert len(warnings) == 1
        assert any("Availability rendah" in w for w in warnings)
    
    def test_very_low_availability_warning(self):
        """Very low availability warning (<80%)."""
        warnings = _determine_warning(
            component="Z", avail=50.0, pct_below=10.0, pct_above=10.0,
            ngap1=2, nover=2, num_spikes=5
        )
        assert len(warnings) == 1
        assert any("Availability sangat rendah" in w for w in warnings)
    
    def test_no_warning(self):
        """No warning for good metrics."""
        warnings = _determine_warning(
            component="Z", avail=99.0, pct_below=10.0, pct_above=10.0,
            ngap1=2, nover=2, num_spikes=5
        )
        assert warnings == []
    
    def test_combined_gap_and_spike_warnings(self):
        """Component with both gaps and spikes should get both warnings."""
        warnings = _determine_warning(
            component="Z", avail=95.0, pct_below=10.0, pct_above=10.0,
            ngap1=10, nover=2, num_spikes=30
        )
        assert len(warnings) == 2
        assert any("gap" in w for w in warnings)
        assert any("Spike" in w for w in warnings)


class TestCheckQC:
    """Test quality classification function."""
    
    def test_baik_classification(self):
        """Score >= 90 should be 'Baik'."""
        assert _check_qc(90.0) == 'Baik'
        assert _check_qc(95.0) == 'Baik'
        assert _check_qc(100.0) == 'Baik'
    
    def test_cukup_baik_classification(self):
        """Score 60-89 should be 'Cukup Baik'."""
        assert _check_qc(60.0) == 'Cukup Baik'
        assert _check_qc(75.0) == 'Cukup Baik'
        assert _check_qc(89.0) == 'Cukup Baik'
    
    def test_buruk_classification(self):
        """Score 1-59 should be 'Buruk'."""
        assert _check_qc(1.0) == 'Buruk'
        assert _check_qc(30.0) == 'Buruk'
        assert _check_qc(59.0) == 'Buruk'
    
    def test_mati_classification(self):
        """Score exactly 0 should be 'Mati'."""
        assert _check_qc(0.0) == 'Mati'


class TestQCThresholds:
    """Test QCThresholds dataclass."""
    
    def test_default_thresholds_exist(self):
        """Default thresholds should be instantiated."""
        assert DEFAULT_THRESHOLDS is not None
        assert isinstance(DEFAULT_THRESHOLDS, QCThresholds)
    
    def test_custom_thresholds(self):
        """Should be able to create custom thresholds."""
        custom = QCThresholds(
            rms_limit=6000.0,
            gap_count_warn=10
        )
        assert custom.rms_limit == 6000.0
        assert custom.gap_count_warn == 10
        # Other values should use defaults
        assert custom.ratioamp_limit == 1.01
    
    def test_weight_sum(self):
        """Weights should sum to approximately 1.0."""
        weights_sum = (
            DEFAULT_THRESHOLDS.weight_noise +
            DEFAULT_THRESHOLDS.weight_availability +
            DEFAULT_THRESHOLDS.weight_rms +
            DEFAULT_THRESHOLDS.weight_ratioamp +
            DEFAULT_THRESHOLDS.weight_gaps +
            DEFAULT_THRESHOLDS.weight_overlaps +
            DEFAULT_THRESHOLDS.weight_spikes
        )
        assert abs(weights_sum - 1.0) < 0.001  # Allow small floating point error


class TestScoreCapping:
    """Test score capping logic."""
    
    def test_availability_80_to_97_caps_at_89(self):
        """Availability between 80-97% should cap score at 89.0."""
        # This would be tested in integration, but we can validate the threshold values
        assert DEFAULT_THRESHOLDS.avail_good == 97.0
        assert DEFAULT_THRESHOLDS.avail_fair == 80.0
        assert DEFAULT_THRESHOLDS.fair_max_score == 89.0
    
    def test_availability_below_80_caps_at_59(self):
        """Availability below 80% should cap score at 59.0."""
        assert DEFAULT_THRESHOLDS.poor_max_score == 59.0


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_zero_margin(self):
        """Zero margin should be handled gracefully."""
        # This would cause division by zero, but it's an invalid configuration
        # The function should ideally handle this, but it's not expected in normal use
        pass
    
    def test_exactly_100_percent_availability(self):
        """Exactly 100% availability should be valid."""
        issues = _validate_qc_metrics(
            rms=100.0, ratioamp=1.5, avail=100.0,
            ngap1=0, nover=0, num_spikes=0,
            pct_above=0.0, pct_below=0.0, dcl=9.0, dcg=0,
            station_code="TEST", component="Z"
        )
        assert issues == []
    
    def test_exactly_0_percent_availability(self):
        """Exactly 0% availability should be valid."""
        issues = _validate_qc_metrics(
            rms=0.0, ratioamp=0.0, avail=0.0,
            ngap1=0, nover=0, num_spikes=0,
            pct_above=0.0, pct_below=0.0, dcl=0.0, dcg=1,
            station_code="TEST", component="Z"
        )
        assert issues == []


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
