import time
import psutil
import logging
from typing import Dict, Optional, Tuple, List

logger = logging.getLogger(__name__)

class RAMManager:
    """
    Manages RAM usage limits, soft starts, and predictive submission control.
    """
    def __init__(self, basic_config: Dict, stations_ram_map: Dict[str, float]):
        self.basic_config = basic_config
        self.stations_map = stations_ram_map
        
        # 0. Default 
        self.ram_limit_bytes = 0.0
        self.default_station_gb = 15.0
        self.allocation_delay = 20.0
        self.soft_start_initial = 6
        self.soft_start_interval = 5.0
        
        # 1. RAM Limits
        if basic_config.get('ram_limit_gb'):
            try:
                gb = float(basic_config['ram_limit_gb'])
                self.ram_limit_bytes = gb * 1024 * 1024 * 1024
                logger.info(f"RAM Limit: {gb} GB")
            except ValueError:
                pass
                
        # 2. Station Weights
        if basic_config.get('ram_station_default_gb'):
            try:
                self.default_station_gb = float(basic_config['ram_station_default_gb'])
            except ValueError:
                pass
                
        # 3. Phantom Load
        if basic_config.get('ram_allocation_delay'):
             try:
                 self.allocation_delay = float(basic_config['ram_allocation_delay'])
             except ValueError:
                 pass
        self.phantom_tasks: List[Tuple[float, float]] = [] # (start_time, gb_estimate)
        
        # 4. Soft Start Configs
        if basic_config.get('ram_soft_start_initial_worker'):
             try:
                 self.soft_start_initial = int(basic_config['ram_soft_start_initial_worker'])
             except ValueError:
                 pass
                 
        if basic_config.get('ram_soft_start_interval'):
             try:
                 self.soft_start_interval = float(basic_config['ram_soft_start_interval'])
             except ValueError:
                 pass

        self.current_concurrency = self.soft_start_initial
        self.last_ramp_time = time.time()
        
        logger.info(f"RAM Manager Init: DefaultStation={self.default_station_gb}GB, Delay={self.allocation_delay}s, SoftStart={self.soft_start_initial}/{self.soft_start_interval}s")

    def get_station_estimate(self, station_tuple: Optional[Tuple]) -> float:
        """Returns RAM estimate for a station in GB."""
        if not station_tuple:
            return 0.0
        try:
            # Tuple: (network, kode, location, sistem_sensor, pref, comp)
            net = station_tuple[0]
            sta = station_tuple[1]
            key = f"{net}.{sta}"
            return self.stations_map.get(key, self.default_station_gb)
        except Exception:
            return self.default_station_gb

    def _update_phantom_load(self):
        """Removes expired phantom load entries."""
        now = time.time()
        self.phantom_tasks = [t for t in self.phantom_tasks if (now - t[0]) < self.allocation_delay]

    def get_phantom_load_bytes(self) -> float:
        self._update_phantom_load()
        gb = sum(t[1] for t in self.phantom_tasks)
        return gb * 1024 * 1024 * 1024

    def check_ram_metrics(self, next_station_tuple: Optional[Tuple] = None) -> Tuple[bool, str]:
        """
        Checks if it is safe to submit the next station.
        Returns: (is_safe, reason_msg)
        """
        if self.ram_limit_bytes <= 0:
            return True, "No Limit"

        # Update phantom
        phantom_bytes = self.get_phantom_load_bytes()
        
        # Get Real Usage
        try:
            mem = psutil.virtual_memory()
            real_used = mem.used
        except:
            real_used = 0
            
        # Estimate Next
        est_gb = self.get_station_estimate(next_station_tuple)
        est_bytes = est_gb * 1024 * 1024 * 1024
        
        projected = real_used + phantom_bytes + est_bytes
        
        if projected < self.ram_limit_bytes:
            return True, "OK"
        else:
            msg = f"RAM Full. Real:{real_used/1024**3:.1f}G + Phantom:{phantom_bytes/1024**3:.1f}G + Next:{est_gb:.1f}G > Limit:{self.ram_limit_bytes/1024**3:.1f}G"
            return False, msg

    def try_ramp_up_concurrency(self, max_processes: int) -> bool:
        """
        Attempts to increase concurrency limit if interval has passed.
        Should only be called if RAM is known to be safe.
        """
        now = time.time()
        if (now - self.last_ramp_time) >= self.soft_start_interval:
            if self.current_concurrency < max_processes:
                self.current_concurrency += 1
                self.last_ramp_time = now
                return True
        return False

    def record_submission(self, station_tuple: Tuple):
        """Records a predictive RAM load for a just-submitted task."""
        est_gb = self.get_station_estimate(station_tuple)
        self.phantom_tasks.append((time.time(), est_gb))

    def get_ram_info(self) -> Tuple[float, float, float]:
        """
        Returns info about current RAM usage.
        Returns: (real_used_gb, phantom_load_gb, ram_limit_gb)
        """
        phantom_bytes = self.get_phantom_load_bytes()
        try:
            mem = psutil.virtual_memory()
            real_used = mem.used
        except:
            real_used = 0
            
        return (
            real_used / (1024**3),
            phantom_bytes / (1024**3),
            self.ram_limit_bytes / (1024**3)
        )
