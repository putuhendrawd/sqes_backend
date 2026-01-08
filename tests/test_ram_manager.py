
import time
import logging
import psutil
from sqes.utils.ram_manager import RAMManager

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MockMem:
    used = 0

def mock_virtual_memory():
    return MockMem()

# Monkey Patch
psutil.virtual_memory = mock_virtual_memory

def test_ram_manager_logic():
    print("--- Testing RAMManager ---")
    
    # Mock Config
    basic_config = {
        'ram_limit_gb': '20.0', # 20GB Limit
        'ram_station_default_gb': '5.0',
        'ram_allocation_delay': '1', # Short delay for testing
        'ram_soft_start_initial_worker': '2',
        'ram_soft_start_interval': '1'
    }
    
    stations_map = {
        'NET.BIG_STATION': 15.0,
        'NET.SMALL_STATION': 2.0
    }
    
    manager = RAMManager(basic_config, stations_map)
    
    # Check Init
    assert manager.ram_limit_bytes == 20.0 * 1024**3
    assert manager.allocation_delay == 1
    assert manager.current_concurrency == 2
    print("[PASS] Initialization")

    # Check Estimates
    est_big = manager.get_station_estimate(('NET', 'BIG_STATION'))
    assert est_big == 15.0
    est_small = manager.get_station_estimate(('NET', 'SMALL_STATION'))
    assert est_small == 2.0
    est_unknown = manager.get_station_estimate(('NET', 'UNKNOWN'))
    assert est_unknown == 5.0 # Default
    print("[PASS] Station Estimates")
    
    # Check Check Logic (Mock psutil used to be 0 for simplicity)
    # 1. Submit BIG (15GB). Real=0. Phantom=0. Total=15. Limit=20. OK.
    safe, msg = manager.check_ram_metrics(('NET', 'BIG_STATION'))
    assert safe == True
    print(f"[PASS] Can submit BIG (15GB) -> {msg}")
    
    # Record it
    manager.record_submission(('NET', 'BIG_STATION'))
    
    # 2. Submit another BIG (15GB). Real=0. Phantom=15. Next=15. Total=30. Limit=20. FAIL.
    # Note: Phantom load is 15GB. Next is 15GB. Total 30GB.
    safe, msg = manager.check_ram_metrics(('NET', 'BIG_STATION'))
    assert safe == False
    print(f"[PASS] Cannot submit 2nd BIG (30GB > 20GB) -> {msg}")
    
    # 3. Submit SMALL (2GB). Real=0. Phantom=15. Next=2. Total=17. Limit=20. OK.
    safe, msg = manager.check_ram_metrics(('NET', 'SMALL_STATION'))
    assert safe == True
    print(f"[PASS] Can submit SMALL (17GB < 20GB) -> {msg}")
    
    # Record SMALL
    manager.record_submission(('NET', 'SMALL_STATION'))
    
    # Check Phantom Load calculation
    phantom_bytes = manager.get_phantom_load_bytes()
    expected_phantom = (15.0 + 2.0) * 1024**3
    # Allow small float error
    assert abs(phantom_bytes - expected_phantom) < 1024 * 1024
    print(f"[PASS] Phantom Load Correct: {phantom_bytes/1024**3:.2f} GB")
    
    # Check Soft Start Ramp
    # Initial was 2. Interval 1s.
    assert manager.current_concurrency == 2
    print("Waiting for ramp up interval...")
    time.sleep(1.1)
    
    ramped = manager.try_ramp_up_concurrency(max_processes=10)
    assert ramped == True
    assert manager.current_concurrency == 3
    print("[PASS] Soft Start Ramped to 3")
    
    # Check Phantom Expiry (Delay=1s)
    # We waited 1.1s. The phantom entries were added before that.
    # Actually logic: 
    # T0: Init
    # T0: Submit Big (Phantom expires T0+1)
    # T0: Submit Small (Phantom expires T0+1)
    # T0+1.1: We sleep.
    # Now T > T0+1. Phantom should be gone.
    
    phantom_bytes = manager.get_phantom_load_bytes()
    assert phantom_bytes == 0
    print(f"[PASS] Phantom Load Expired: {phantom_bytes}")

    # Check get_ram_info
    real_gb, phantom_gb, limit_gb = manager.get_ram_info()
    assert real_gb == 0.0 # Mocked
    assert phantom_gb == 0.0 # Expired
    assert abs(limit_gb - 20.0) < 0.01 
    print(f"[PASS] get_ram_info keys check: {real_gb}, {phantom_gb}, {limit_gb}")

if __name__ == "__main__":
    test_ram_manager_logic()
