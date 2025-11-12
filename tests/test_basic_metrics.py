# tests/test_basic_metrics.py
import pytest
import numpy as np
from obspy import Stream, Trace, UTCDateTime

# Import the *only* public function you want to test
from sqes.core.basic_metrics import process_basic_metrics

# --- Fixtures: Reusable Test Data (Unchanged) ---

@pytest.fixture
def sample_stream():
    """A simple, 1-trace stream for basic tests."""
    # Data: [0, 1, 2, 3, 4, 5]
    # Mean: 2.5
    # Data-Mean: [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5]
    # (Data-Mean)^2: [6.25, 2.25, 0.25, 0.25, 2.25, 6.25]
    # Mean of squares: 17.5 / 6 = 2.91666...
    # RMS (std dev): sqrt(2.91666...) = 1.707825...
    data = np.array([0., 1., 2., 3., 4., 5.])
    tr = Trace(
        data=data, 
        header={'starttime': UTCDateTime("1970-01-01T00:00:00Z"), 'sampling_rate': 1.0}
    )
    return Stream(traces=[tr])

@pytest.fixture
def stream_with_gap():
    """A 2-trace stream with a gap."""
    tr1 = Trace(
        data=np.ones(5), 
        header={'starttime': UTCDateTime("1970-01-01T00:00:00Z"), 'sampling_rate': 1.0} # Ends at 4s
    )
    tr2 = Trace(
        data=np.ones(5), 
        header={'starttime': UTCDateTime("1970-01-01T00:00:10Z"), 'sampling_rate': 1.0} # Starts at 10s
    )
    return Stream(traces=[tr1, tr2])

@pytest.fixture
def stream_with_spike():
    """A 1000-sample stream with one big spike in the middle."""
    data = np.zeros(1000, dtype=np.float64)
    data[500] = 100.0 # Spike
    tr = Trace(data=data, header={'starttime': UTCDateTime(0), 'sampling_rate': 1.0})
    return Stream(traces=[tr])

@pytest.fixture
def stream_with_edge_spike():
    """A 1000-sample stream with a spike near the beginning (should be ignored)."""
    data = np.zeros(1000, dtype=np.float64)
    data[10] = 100.0 # Spike at edge
    tr = Trace(data=data, header={'starttime': UTCDateTime(0), 'sampling_rate': 1.0})
    return Stream(traces=[tr])

# --- New Test Function ---

def test_process_basic_metrics(sample_stream, stream_with_gap):
    # Define the 24-hour window
    t0 = UTCDateTime("1970-01-01T00:00:00Z")
    t1 = UTCDateTime("1970-01-01T23:59:59Z")
    day_duration = t1 - t0 # 86399.0

    # --- Test 1: sample_stream ---
    metrics = process_basic_metrics("file.mseed", sample_stream, t0, t1, spike_method='fast')

    # Check RMS (which is now Standard Deviation)
    # 1.707825... rounded to 2 decimal places is 1.71
    assert metrics['rms'] == "1.71"
    
    # Check Amplitudes
    assert metrics['ampmax'] == "5.0"
    assert metrics['ampmin'] == "0.0"
    
    # Check Ratio (abs(0) or abs(5) is 0, so ratio is 1.0)
    assert metrics['ratioamp'] == "1.0"
    
    # Check Availability
    # Stream span = 5s. Day span = 86399s. (100 * 5 / 86399) = 0.00578...
    assert metrics['psdata'] == "0.01" # 0.00578... rounds to 0.01
    
    # Check Gaps/Overlaps/Spikes
    assert metrics['ngap'] == "0"
    assert metrics['nover'] == "0"
    assert metrics['num_spikes'] == "0"
    
    # --- Test 2: stream_with_gap ---
    metrics_gap = process_basic_metrics("file.mseed", stream_with_gap, t0, t1, spike_method='fast')
    
    # Check Gaps/Overlaps (1 gap, 0 overlaps)
    assert metrics_gap['ngap'] == "1"
    assert metrics_gap['nover'] == "0"

def test_process_basic_metrics_spikes(stream_with_spike, stream_with_edge_spike):
    t0 = UTCDateTime("1970-01-01T00:00:00Z")
    t1 = UTCDateTime("1970-01-01T23:59:59Z")
    
    # --- Test 1: Spike in the middle (using 'fast' method) ---
    metrics_fast = process_basic_metrics(
        "file.mseed", stream_with_spike, t0, t1, spike_method='fast'
    )
    assert metrics_fast['num_spikes'] == "1"
    
    # --- Test 2: Spike in the middle (using 'efficient' method) ---
    metrics_efficient = process_basic_metrics(
        "file.mseed", stream_with_spike, t0, t1, spike_method='efficient'
    )
    assert metrics_efficient['num_spikes'] == "1"
    
    # --- Test 3: Spike at the edge (should be 0 for both methods) ---
    metrics_edge = process_basic_metrics(
        "file.mseed", stream_with_edge_spike, t0, t1, spike_method='fast'
    )
    assert metrics_edge['num_spikes'] == "0"
    
    metrics_edge_eff = process_basic_metrics(
        "file.mseed", stream_with_edge_spike, t0, t1, spike_method='efficient'
    )
    assert metrics_edge_eff['num_spikes'] == "0"