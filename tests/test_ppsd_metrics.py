# tests/test_ppsd_metrics.py
import pytest
import numpy as np

# Import the *private* helper functions we want to unit test
from sqes.core.ppsd_metrics import (
    _dead_channel_gsn, 
    _percentage_outside_model, 
    _percentage_inside_model_by_period, 
    _dead_channel_lin
)

# --- Tests for _dead_channel_gsn ---

def test_dead_channel_gsn_is_dead():
    """Test the 'dead' case (deviation > 5)"""
    # Y(x)
    psd = np.array([10, 10, 10])
    # NLNM
    model = np.array([16, 17, 18])
    # Periods (all in 4-8s band)
    t = np.array([4.1, 5.0, 7.9])
    
    # Deviation = mean( (16-10) + (17-10) + (18-10) ) = mean(6, 7, 8) = 7.0
    # Since 7.0 > 5.0, it should return 1 (dead)
    assert _dead_channel_gsn(psd, model, t) == 1

def test_dead_channel_gsn_is_ok():
    """Test the 'good' case (deviation <= 5)"""
    # Y(x)
    psd = np.array([10, 10, 10])
    # NLNM
    model = np.array([11, 12, 13])
    # Periods
    t = np.array([4.1, 5.0, 7.9])
    
    # Deviation = mean( (11-10) + (12-10) + (13-10) ) = mean(1, 2, 3) = 2.0
    # Since 2.0 <= 5.0, it should return 0 (ok)
    assert _dead_channel_gsn(psd, model, t) == 0

def test_dead_channel_gsn_empty():
    """Test an empty slice (no data in 4-8s band)"""
    psd = np.array([10, 10])
    model = np.array([20, 20])
    t = np.array([1.0, 2.0]) # No data in 4-8s band
    
    # The function should return 0 (not 0.0)
    assert _dead_channel_gsn(psd, model, t) == 0

# --- Tests for _percentage_outside_model ---

def test_percentage_outside_model():
    """Tests the calculation of percentages above NHNM and below NLNM."""
    psd =   np.array([ -170, -110, -130, -90 ]) # 4 points total
    nhnm =  np.array([ -120, -120, -125, -100 ])
    nlnm =  np.array([ -160, -130, -135, -120 ])
    
    # Point 0 (-170): IS < nlnm (-160) -> 1 for percL
    # Point 1 (-110): IS > nhnm (-120) -> 1 for percH
    # Point 2 (-130): Is between -135 and -125.
    # Point 3 ( -90): IS > nhnm (-100) -> 1 for percH
    
    # percH = 2, percL = 1. Total = 4
    # Expected: (100 * 2/4) = 50.0, (100 * 1/4) = 25.0
    pctH, pctL = _percentage_outside_model(psd, nhnm, nlnm)
    
    assert pctH == 50.0
    assert pctL == 25.0

def test_percentage_outside_model_empty():
    """Tests an empty array"""
    pctH, pctL = _percentage_outside_model(np.array([]), np.array([]), np.array([]))
    assert pctH == 0.0
    assert pctL == 0.0

# --- Tests for _percentage_inside_model_by_period ---

def test_percentage_inside_model_by_period():
    """Tests the calculation of percentage *inside* the model for a slice."""
    psd =   np.array([ 10,  20,  30,  40,  100, 60 ]) # 6 points
    nhnm =  np.array([ 15,  25,  35,  45,  55,  65 ])
    nlnm =  np.array([ 5,   15,  25,  35,  45,  55 ])
    t =     np.array([ 1,   2,   3,   4,   5,   6  ])
    
    # Test a band from t=1.5 to t=5.5.
    # This selects t = [2, 3, 4, 5] (4 points)
    # psd_slice  = [20, 30, 40, 100]
    # nhnm_slice = [25, 35, 45, 55]
    # nlnm_slice = [15, 25, 35, 45]
    
    # Point 1 (20): 15 < 20 < 25 (True)
    # Point 2 (30): 25 < 30 < 35 (True)
    # Point 3 (40): 35 < 40 < 45 (True)
    # Point 4 (100): Is > 55 (False)
    
    # 3 out of 4 points are inside. Expected: 75.0%
    perc = _percentage_inside_model_by_period(psd, nlnm, nhnm, t, t0=1.5, t1=5.5)
    assert perc == 75.0

def test_percentage_inside_model_by_period_empty():
    """Tests an empty slice (no data in period band)"""
    psd =   np.array([ 10, 20 ])
    nhnm =  np.array([ 15, 25 ])
    nlnm =  np.array([ 5,  15 ])
    t =     np.array([ 1,  2  ])
    
    # No data between t=5 and t=10
    perc = _percentage_inside_model_by_period(psd, nlnm, nhnm, t, t0=5, t1=10)
    assert perc == 0.0

# --- Tests for _dead_channel_lin ---

def test_dead_channel_lin_perfect_fit():
    """
    Tests the RMSE of a perfect line (should be 0).
    y = 2x + 10 (where x = log10(t))
    """
    # Test data:
    t_log = np.array([0.0, 1.0, 1.5]) # log10(period)
    t = 10.0**t_log                   # [1.0, 10.0, 31.62] (Use 10.0 to make it float)
    psd = (2 * t_log) + 10            # [10.0, 12.0, 13.0]
    
    fs = 100.0 # t0 = 4/100 = 0.04. All points are valid.
    
    dcl = _dead_channel_lin(psd, t, fs)
    
    # The RMSE of a perfect fit must be 0
    assert dcl == pytest.approx(0.0)

def test_dead_channel_lin_noisy_fit():
    """
    Tests the RMSE of a noisy line.
    y = [11, 11, 15]
    x (log-period) = [0, 1, 1.9]
    """
    t_log = np.array([0.0, 1.0, 1.9])
    t = 10.0**t_log                     # [1.0, 10.0, 79.43] (Use 10.0)
    psd_noisy = np.array([11.0, 11.0, 15.0])
    
    fs = 100.0 # t0 = 0.04, t1 = 100.0. All points are valid.

    # Manually calculate the expected RMSE
    # polyfit([0, 1, 1.9], [11, 11, 15]) -> slope=2.0667..., intercept=10.3333...
    slope = 2.0667
    intercept = 10.3333
    
    psdfit = intercept + slope * t_log

    # residuals**2 = (psdfit - psd_noisy)**2
    residuals_sq = (psdfit - psd_noisy)**2
    
    expected_value = np.sqrt(np.mean(residuals_sq)) 
    
    dcl = _dead_channel_lin(psd_noisy, t, fs)
    assert dcl == pytest.approx(expected_value, rel=1e-5)
    assert dcl > 0.0 # Must be greater than 0

def test_dead_channel_lin_period_trim():
    """
    Tests that the function correctly trims data outside the (4/fs, 100) range.
    """
    fs = 40.0
    # t0 = 4.0 / 40.0 = 0.1
    # t1 = 100.0
    
    # We must use 10.0 (float) to allow negative powers (FIXED)
    t_log = np.array([-2.0, -1.0, 0.0, 1.0, 2.0]) 
    t = 10.0**t_log # [0.01, 0.1, 1.0, 10.0, 100.0]
    
    # The first two and last points are "noise" and should be excluded
    psd = np.array([1000.0, 1000.0, 10.0, 12.0, 1000.0])
    
    # The mask (t > 0.1) & (t < 100.0) should select:
    # t_slice = [1.0, 10.0]
    # psd_slice = [10.0, 12.0]
    
    # The fit for these two points is a perfect line (y=2x+10)
    # The RMSE should be 0.0
    dcl = _dead_channel_lin(psd, t, fs)
    assert dcl == pytest.approx(0.0)