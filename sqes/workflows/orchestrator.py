"""Main workflow orchestrator for processing seismic data over date ranges."""
import logging
from datetime import datetime, timedelta
from typing import Any, Optional, Dict

from .daily_processor import run_single_day

logger = logging.getLogger(__name__)


def run_processing_workflow(start_date_str: str, end_date_str: str, 
                            stations: Optional[list], network: Optional[list],
                            ppsd: bool, mseed: bool, flush: bool, log_level: int,
                            log_file_path: str,
                            basic_config: Dict[str, Any]):
    """
    Orchestrates processing for all or specific stations over a date range.
    
    This is the main entry point called by cli.py.
    
    Args:
        start_date_str: Start date in YYYYMMDD format
        end_date_str: End date in YYYYMMDD format
        stations: Optional list of station codes to process (None = all stations)
        ppsd: Whether to save PPSD matrices as NPZ files
        mseed: Whether to save downloaded waveforms as MiniSEED
        flush: Whether to flush existing data before processing
        log_level: Logging level (INFO, DEBUG, etc.)
        log_file_path: Path to the log file for worker processes
        basic_config: Basic configuration dictionary
    """
    logger.info(f"--- Starting Main Workflow from {start_date_str} to {end_date_str} ---")
    
    try:
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
        
        if start_date > end_date:
            logger.error("Start date must be before or the same as end date.")
            return
            
    except ValueError as e:
        logger.error(f"Invalid date format: {e}. Use YYYYMMDD.")
        return
        
    # --- Date Loop ---
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        logger.info(f"==================================================")
        logger.info(f"Processing date: {date_str}")
        logger.info(f"==================================================")
        
        # Flush is only allowed if the user specified --flush AND
        # it's a single-day run (start_date == end_date).
        # cli.py already blocks this, but we double-check.
        do_flush = (flush and (start_date == end_date))
        
        try:
            # Call the internal single-day processor
            run_single_day(
                date_str=date_str,
                ppsd=ppsd,
                mseed=mseed,
                flush=do_flush, 
                log_level=log_level,
                log_file_path=log_file_path,
                stations=stations,
                network=network,
                basic_config=basic_config 
            )
        except Exception as e:
            logger.error(f"Failed to process {date_str}: {e}. Skipping to next date.")

            
        current_date += timedelta(days=1)
        
    logger.info("--- Main Workflow Finished ---")
