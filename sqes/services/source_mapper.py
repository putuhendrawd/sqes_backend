"""
Source mapping service for per-station data source configuration.

This module handles loading and resolving station-specific waveform and inventory
sources from source.cfg file.
"""

import os
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Cache for source mapping to avoid repeated file I/O
_SOURCE_MAPPING_CACHE: Optional[Dict[Tuple[str, str], 'StationSourceConfig']] = None


@dataclass
class WaveformSourceConfig:
    """Configuration for waveform data source."""
    type: str  # 'fdsn' or 'sds'
    tag: str   # Section name in global.cfg (e.g., 'client', 'client2', 'archive', 'archive2')


@dataclass
class InventorySourceConfig:
    """Configuration for inventory data source."""
    type: str  # 'fdsn' or 'local'
    tag: str   # Section name in global.cfg (e.g., 'inventory_client', 'inventory', etc.)


@dataclass
class StationSourceConfig:
    """Combined waveform and inventory source configuration for a station."""
    waveform: Optional[WaveformSourceConfig] = None
    inventory: Optional[InventorySourceConfig] = None


def load_source_mapping(filename: str = 'source.cfg') -> Dict[Tuple[str, str], StationSourceConfig]:
    """
    Load station-to-source mapping from source.cfg file.
    
    File format:
    NETWORK STATION WAVEFORM_TYPE WAVEFORM_TAG [INVENTORY_TYPE INVENTORY_TAG]
    
    The 'default' keyword can be used for type/tag to use global.cfg defaults.
    
    Args:
        filename: Name of the source mapping file (default: 'source.cfg')
        
    Returns:
        Dictionary mapping (network, station) tuples to StationSourceConfig objects
    """
    global _SOURCE_MAPPING_CACHE
    
    # Check cache first
    if _SOURCE_MAPPING_CACHE is not None:
        return _SOURCE_MAPPING_CACHE
    
    # Build path to config file
    module_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(module_path, '..', '..', 'config', filename)
    
    mapping: Dict[Tuple[str, str], StationSourceConfig] = {}
    
    # If file doesn't exist, return empty mapping
    if not os.path.exists(config_path):
        logger.info(f"Source mapping file not found at {config_path}. Using global.cfg defaults for all stations.")
        _SOURCE_MAPPING_CACHE = mapping
        return mapping
    
    logger.debug(f"Loading source mapping from {config_path}")
    
    with open(config_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            # Skip comments and empty lines
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            parts = line.split()
            
            # Validate line format
            if len(parts) < 4:
                logger.warning(f"Invalid source.cfg line {line_num}: '{line}' - Expected at least 4 fields")
                continue
            
            network = parts[0]
            station = parts[1]
            waveform_type = parts[2]
            waveform_tag = parts[3]
            
            # Parse waveform source
            waveform_config: Optional[WaveformSourceConfig] = None
            if waveform_type.lower() != 'default' and waveform_tag.lower() != 'default':
                if waveform_type not in ['fdsn', 'sds']:
                    logger.warning(f"Invalid waveform type '{waveform_type}' on line {line_num}. Expected 'fdsn', 'sds', or 'default'")
                    continue
                waveform_config = WaveformSourceConfig(type=waveform_type, tag=waveform_tag)
            
            # Parse inventory source (optional)
            inventory_config: Optional[InventorySourceConfig] = None
            if len(parts) >= 6:
                inventory_type = parts[4]
                inventory_tag = parts[5]
                
                if inventory_type.lower() != 'default' and inventory_tag.lower() != 'default':
                    if inventory_type not in ['fdsn', 'local']:
                        logger.warning(f"Invalid inventory type '{inventory_type}' on line {line_num}. Expected 'fdsn', 'local', or 'default'")
                        continue
                    inventory_config = InventorySourceConfig(type=inventory_type, tag=inventory_tag)
            
            # Store mapping
            key = (network, station)
            mapping[key] = StationSourceConfig(waveform=waveform_config, inventory=inventory_config)
            
            logger.debug(f"Mapped {network}.{station}: waveform={waveform_config}, inventory={inventory_config}")
    
    logger.debug(f"Loaded {len(mapping)} station source mappings from {config_path}")
    
    # Cache the result
    _SOURCE_MAPPING_CACHE = mapping
    return mapping


def get_station_sources(network: str, station: str, 
                       filename: str = 'source.cfg') -> Optional[StationSourceConfig]:
    """
    Get the source configuration for a specific station.
    
    Args:
        network: Network code
        station: Station code
        filename: Name of the source mapping file (default: 'source.cfg')
        
    Returns:
        StationSourceConfig if station is in source.cfg, None otherwise
    """
    mapping = load_source_mapping(filename)
    key = (network, station)
    return mapping.get(key)


def clear_cache():
    """Clear the source mapping cache. Useful for testing or config reloads."""
    global _SOURCE_MAPPING_CACHE
    _SOURCE_MAPPING_CACHE = None
    logger.debug("Source mapping cache cleared")
