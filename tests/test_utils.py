# tests/test_utils.py
import pytest
import numpy as np
from obspy import Stream, Trace

# Import the function we want to test
from sqes.core import utils

@pytest.fixture
def stream_with_locations():
    """A fixture to create a Stream with various location codes."""
    # Create traces with duplicate, multiple, and empty location codes
    tr1 = Trace(header={'location': '00'})
    tr2 = Trace(header={'location': '10'})
    tr3 = Trace(header={'location': '00'}) # Duplicate
    tr4 = Trace(header={'location': ''})   # Empty string
    
    return Stream(traces=[tr1, tr2, tr3, tr4])

def test_get_location_info(stream_with_locations):
    """
    Tests that the get_location_info function correctly finds,
    uniques, and sorts the location codes from a Stream.
    """
    # Call the function
    locations = utils.get_location_info(stream_with_locations)
    
    # Define the expected result (must be sorted alphabetically)
    expected = np.array(['', '00', '10'])
    
    # Check that the type is correct (NumPy array)
    assert isinstance(locations, np.ndarray)
    
    # Check that the value is correct
    assert np.array_equal(locations, expected)

def test_get_location_info_empty():
    """
    Tests that the function returns an empty array for an empty stream.
    """
    st_empty = Stream()
    locations = utils.get_location_info(st_empty)
    
    assert isinstance(locations, np.ndarray)
    assert len(locations) == 0