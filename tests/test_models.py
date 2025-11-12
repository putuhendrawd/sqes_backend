# tests/test_models.py
import pytest
import numpy as np
from sqes.core import models

@pytest.fixture
def standard_powers():
    """
    A pytest fixture to provide the standard 'powers' array.
    """
    return sorted(range(-190, -90 + 1), reverse=True)

def test_get_models_basic_values(standard_powers):
    """
    Tests that the function calculates the correct dB values for
    a few known points from the Peterson (1993) model.
    """
    periods = np.array([1.0, 10.0])
    
    nhnm, nlnm, p_idx = models.get_models(periods, standard_powers)
    
    assert len(nhnm) == 2
    assert len(nlnm) == 2
    assert p_idx == [0, 1]

    # --- Verify NLNM (New Low Noise Model) ---
    # At T=1.0s, log10(1)=0. Index=3. Model is Al[3] + Bl[3]*0 = -166.40
    assert nlnm[0] == pytest.approx(-166.40)
    # At T=10.0s, log10(10)=1. Index=8. Model is Al[8] + Bl[8]*1 = -97.26 - 66.49 = -163.75
    assert nlnm[1] == pytest.approx(-163.75)

    # --- Verify NHNM (New High Noise Model) ---
    # At T=1.0s, log10(1)=0. Index=3. Model is Ah[3] + Bh[3]*0 = -116.85
    assert nhnm[0] == pytest.approx(-116.85)
    # At T=10.0s, log10(10)=1. Index=7. Model is Ah[7] + Bh[7]*1 = -93.37 - 22.42 = -115.79
    assert nhnm[1] == pytest.approx(-115.79) # <--- THIS WAS THE FAILED ASSERTION

def test_get_models_trimming(standard_powers):
    """
    Tests that the function correctly ignores periods that are
    outside the model's defined range (0.1s to 100000s).
    """
    periods = np.array([
        0.01,       # Index 0 (Too short) -> Ignored
        0.05,       # Index 1 (Too short) -> Ignored
        1.0,        # Index 2 (Valid)
        10.0,       # Index 3 (Valid)
        200000.0    # Index 4 (Too long) -> Ignored 
    ])
    
    nhnm, nlnm, p_idx = models.get_models(periods, standard_powers)
    
    # Should only get results for the 2 valid periods
    assert len(nhnm) == 2
    assert len(nlnm) == 2
    
    # The indices returned MUST match the indices from the *original* array
    assert p_idx == [2, 3]
    
    # Check that the values are for T=1.0s (nlnm[0]) and T=10.0s (nhnm[1])
    assert nlnm[0] == pytest.approx(-166.40)
    assert nhnm[1] == pytest.approx(-115.79) # <--- THIS WAS THE OTHER FAILED ASSERTION

def test_get_models_empty(standard_powers):
    """
    Tests that the function returns empty results for empty input.
    """
    periods = np.array([])
    
    nhnm, nlnm, p_idx = models.get_models(periods, standard_powers)
    
    assert len(nhnm) == 0
    assert len(nlnm) == 0
    assert len(p_idx) == 0