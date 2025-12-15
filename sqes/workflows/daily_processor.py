"""Daily processing workflow for seismic stations."""
import time
import logging
import multiprocessing
from functools import partial
from datetime import datetime
from typing import Any, Optional, Dict

from ..services.db_pool import DBPool
from ..services.repository import QCRepository
from .station_processor import process_station_data
from ..analysis import qc_analyzer
from .helpers import (
    get_common_configs,
    setup_paths_and_times,
    get_output_paths,
    calculate_process_count,
    load_qc_thresholds
)

logger = logging.getLogger(__name__)


def run_single_day(date_str: str, ppsd: bool, flush: bool, mseed: bool,
                    log_level: int, log_file_path: str, basic_config: Dict[str, Any],
                    stations: Optional[list] = None, network: Optional[list] = None):
    """
    Orchestrates the processing of stations for a single day.
    
    If 'stations' is None, processes all stations.
    If 'stations' is a list, processes only those stations.
    
    Args:
        date_str: Date string in YYYYMMDD format
        ppsd: Whether to save PPSD matrices as NPZ files
        flush: Whether to flush existing data before processing
        mseed: Whether to save downloaded waveforms as MiniSEED
        log_level: Logging level (INFO, DEBUG, etc.)
        log_file_path: Path to the log file for worker processes
        basic_config: Basic configuration dictionary
        stations: Optional list of station codes to process
    """
    logger.info(f"--- Starting Daily Run for {date_str} ---")
    if stations:
        logger.info(f"--- Filtering for stations: {stations} ---")
    if network:
        logger.info(f"--- Filtering for network: {network} ---")
    dt_start = datetime.now()
    
    # --- 1. Setup ---
    try:
        basic_config, db_type, client_creds, db_creds = get_common_configs(basic_config)
        time0, time1, tgl, tahun = setup_paths_and_times(date_str)
        output_paths = get_output_paths(basic_config, tahun, tgl, date_str)
    except Exception as e:
        logger.error(f"Failed to setup workflow for {date_str}: {e}")
        return

    # This loop is to catch incomplete processing
    run_trigger = 1 
    
    # Load QC thresholds once for the entire workflow
    qc_thresholds = load_qc_thresholds()
    logger.debug("QC thresholds loaded for workflow")
    
    # If a station list or network filter is provided, we *never* loop. We just run once.
    if stations or network:
        run_trigger = -1 # Special flag to run once and exit
    
    while run_trigger > 0 or run_trigger == -1:
        if stations or network:
            logger.info(f"--- Processing specified stations/network for {tgl} ---")
            run_trigger = 0 # Set to 0 so it exits after this one pass
        else:
            logger.info(f"--- Processing loop, Pass {run_trigger} for {tgl} ---")

        try:
            db_pool = DBPool(**db_creds)
            repo = QCRepository(db_pool, db_type)
        except Exception as e:
            logger.error(f"Failed to initialize DB: {e}. Retrying in 10s...")
            time.sleep(10)
            if stations or network: break # Don't retry if user specified stations/network
            continue
            
        # --- 2. Flush (if requested) ---
        if flush:
            logger.info(f"Flushing data for {tgl}...")
            repo.flush_daily_data(tgl)
            logger.info("Flush success!")
            flush = False # Only flush on the first pass
        
        # --- 3. Get Data to Process ---
        if stations:
            # We have a specific list: get tuples for them.
            logger.info(f"Querying for {len(stations)} specific stations...")
            data = repo.get_station_tuples(stations, network=network) 
        else:
            # No list: get all unprocessed stations.
            logger.info("Querying for all unprocessed stations...")
            data = repo.get_stations_to_process(tgl, network=network)
        
        if data is None:
            logger.error("Error querying stations. Retrying...")
            del(db_pool)
            time.sleep(10)
            if stations or network: break # Don't retry
            continue
            
        logger.info(f"Found {len(data)} stations to process.")

        # --- 4. Run Multiprocessing ---
        if data:
            if basic_config.get('cpu_number_used'):
                processes_req = int(basic_config['cpu_number_used'])
            else:
                processes_req = calculate_process_count(len(data) // 35)
            logger.info(f"Starting multiprocessing pool with {processes_req} workers.")
            
            del(db_pool) # Close main pool before forking

            process_func = partial(
                process_station_data,
                tgl=tgl, time0=time0, time1=time1,
                client_credentials=client_creds,
                db_credentials=db_creds,
                basic_config=basic_config,
                output_paths=output_paths,
                pdf_trigger=ppsd,
                mseed_trigger=mseed,
                log_level=log_level,
                log_file_path=log_file_path,
                qc_thresholds=qc_thresholds
            )
            with multiprocessing.Pool(processes=processes_req) as pool:
                pool.map(process_func, data)
        else:
            logger.info(f"No stations to process for {tgl}.")

        # --- 5. Post-Processing & Straggler Check ---
        logger.info(f"Checking for stragglers on {tgl}...")
        db_pool = DBPool(**db_creds) # Reconnect
        repo = QCRepository(db_pool, db_type)
        
        # Pass the station list (which is None or a list)
        data_stragglers = repo.get_straggler_stations(tgl, station_list=stations)
        
        if data_stragglers is None:
            logger.error(f"Failed to query for stragglers (DB error?). Skipping straggler check for {tgl}.")
        else:
            # Pylance is now happy, len() is safe
            logger.info(f"Found {len(data_stragglers)} stations for final QC Analysis.")
            if data_stragglers:
                for sta in data_stragglers:
                    kode_qc = sta[0]
                    logger.debug(f"Running QC Analysis for straggler: {kode_qc}")
                    qc_analyzer.run_qc_analysis(repo, db_type, tgl, kode_qc, qc_thresholds)
            else:
                logger.info(f"No stragglers found.")
        
        if data_stragglers:
            for sta in data_stragglers:
                kode_qc = sta[0]
                logger.debug(f"Running QC Analysis for straggler: {kode_qc}")
                qc_analyzer.run_qc_analysis(repo, db_type, tgl, kode_qc, qc_thresholds)
        else:
            logger.info(f"No stragglers found.")
            
        # --- 6. Final Completion Check ---
        # If we specified stations, we *always* exit now.
        if stations or network:
            logger.info("Specified stations/network processed. Day complete.")
            run_trigger = 0 # Force exit
        else:
            # If we are running for ALL stations, we check for completeness.
            logger.info("Checking for overall completion...")
            data_a = repo.get_stations_to_process(tgl)
            data_b = repo.get_straggler_stations(tgl, station_list=None) 
            
            if data_a is None or data_b is None:
                logger.error(f"Failed to get completion data for {tgl} (DB error?). Cannot re-run. Exiting loop for this day.")
                run_trigger = 0 # Break loop, something is wrong
            else:
                # Pylance is now happy, len() is safe
                if (len(data_a) > 0) or (len(data_b) > 0):
                    logger.warning(f"Incomplete data. Re-running loop (Pass {run_trigger}).")
                    logger.warning(f"  {len(data_a)} stations pending processing.")
                    logger.warning(f"  {len(data_b)} stations pending analysis.")
                    run_trigger += 1
                    time.sleep(10)
                    if run_trigger >= 5: # Safety break
                        logger.error(f"Failed to complete processing for {tgl} after 5 attempts.")
                        run_trigger = 0
                else:
                    logger.info(f"All processing and analysis for {tgl} is complete.")
                    run_trigger = 0 # All done

    dt_end = datetime.now()
    logger.info(f"--- Daily Run for {date_str} Finished ({dt_end-dt_start}) ---")
