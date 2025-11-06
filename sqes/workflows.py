import os
import time
import logging
import multiprocessing
from functools import partial
from datetime import datetime, timedelta
from typing import Any, Optional, Dict
from obspy import UTCDateTime

from sqes.services.config_loader import load_config
from sqes.services.db_pool import DBPool
from sqes.services.file_system import create_directory
from sqes.analysis.repository import QCRepository
from sqes.analysis.worker import process_station_data
from sqes.analysis import qc_analyzer

logger = logging.getLogger(__name__)

# --- Helper functions for workflows ---

def _get_common_configs(basic_config):
    """Loads all necessary configs."""
    db_type = basic_config['use_database']
    client_credentials = load_config(section='client')
    db_credentials = load_config(section=db_type)
    return basic_config, db_type, client_credentials, db_credentials

def _setup_paths_and_times(date_str):
    """Generates paths and time objects for a given date."""
    wkt1 = time.strptime(date_str, "%Y%m%d")
    time0 = UTCDateTime(date_str)
    tgl = time0.strftime("%Y-%m-%d")
    time1 = time0 + 86400
    tahun = wkt1.tm_year
    
    return time0, time1, tgl, tahun

def _get_output_paths(basic_config, tahun, tgl, date_str):
    """Creates and returns a dictionary of output paths."""
    outputPSD = os.path.join(basic_config['outputpsd'], str(tahun), date_str)
    outputPDF = os.path.join(basic_config['outputpdf'], str(tgl))
    outputsignal = os.path.join(basic_config['outputsignal'], str(tgl))
    outputmseed = os.path.join(basic_config['outputmseed'], str(tgl))
    
    create_directory(outputsignal)
    create_directory(outputPDF)
    create_directory(outputmseed)
    create_directory(outputPSD)
    
    return {
        'outputPSD': outputPSD,
        'outputPDF': outputPDF,
        'outputsignal': outputsignal,
        'outputmseed': outputmseed
    }

def _processes_round(x, base=2):
    """Calculates the number of processes to use."""
    min_value = 4
    # Use max(1, ...) to avoid 0 CPUs on small machines
    max_value = max(1, multiprocessing.cpu_count() // 3)
    rounded_value = base * round(x / base)
    
    # Clamp the value between min and max
    rounded = max(1, min(max_value, max(min_value, rounded_value)))
    return rounded


# --- Main Public Workflow ---

def run_processing_workflow(start_date_str: str, end_date_str: str, 
                            stations: Optional[list], npz: bool, 
                            flush: bool, log_level: int,
                            basic_config: Dict[str, Any]):
    """
    Orchestrates processing for all or specific stations over a date range.
    
    This is the main entry point called by cli.py.
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
            _run_single_day(
                date_str=date_str,
                npz=npz,
                flush=do_flush, 
                log_level=log_level,
                stations=stations,
                basic_config=basic_config 
            )
        except Exception as e:
            logger.error(f"Failed to process {date_str}: {e}. Skipping to next date.", exc_info=True)
            
        current_date += timedelta(days=1)
        
    logger.info("--- Main Workflow Finished ---")


# --- Internal Daily Processor ---

def _run_single_day(date_str: str, npz: bool, flush: bool, 
                    log_level: int, basic_config: Dict[str, Any],
                    stations: Optional[list] = None):
    """
    Orchestrates the processing of stations for a single day.
    If 'stations' is None, processes all stations.
    If 'stations' is a list, processes only those stations.
    """
    logger.info(f"--- Starting Daily Run for {date_str} ---")
    if stations:
        logger.info(f"--- Filtering for stations: {stations} ---")
    dt_start = datetime.now()
    
    # --- 1. Setup ---
    try:
        basic_config, db_type, client_creds, db_creds = _get_common_configs(basic_config)
        time0, time1, tgl, tahun = _setup_paths_and_times(date_str)
        output_paths = _get_output_paths(basic_config, tahun, tgl, date_str)
    except Exception as e:
        logger.error(f"Failed to setup workflow for {date_str}: {e}", exc_info=True)
        return

    # This loop is to catch incomplete processing
    run_trigger = 1 
    
    # If a station list is provided, we *never* loop. We just run once.
    if stations:
        run_trigger = -1 # Special flag to run once and exit
    
    while run_trigger > 0 or run_trigger == -1:
        if stations:
            logger.info(f"--- Processing specified stations for {tgl} ---")
            run_trigger = 0 # Set to 0 so it exits after this one pass
        else:
            logger.info(f"--- Processing loop, Pass {run_trigger} for {tgl} ---")

        try:
            db_pool = DBPool(**db_creds)
            repo = QCRepository(db_pool, db_type)
        except Exception as e:
            logger.error(f"Failed to initialize DB: {e}. Retrying in 10s...")
            time.sleep(10)
            if stations: break # Don't retry if user specified stations
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
            data = repo.get_station_tuples(stations) 
        else:
            # No list: get all unprocessed stations.
            logger.info("Querying for all unprocessed stations...")
            data = repo.get_stations_to_process(tgl)
        
        if data is None:
            logger.error("Error querying stations. Retrying...")
            del(db_pool)
            time.sleep(10)
            if stations: break # Don't retry
            continue
            
        logger.info(f"Found {len(data)} stations to process.")

        # --- 4. Run Multiprocessing ---
        if data:
            if basic_config.get('cpu_number_used'):
                processes_req = int(basic_config['cpu_number_used'])
            else:
                processes_req = _processes_round(len(data) // 35)
            logger.info(f"Starting multiprocessing pool with {processes_req} workers.")
            
            del(db_pool) # Close main pool before forking

            process_func = partial(
                process_station_data,
                tgl=tgl, time0=time0, time1=time1,
                client_credentials=client_creds,
                db_credentials=db_creds,
                basic_config=basic_config,
                output_paths=output_paths,
                pdf_trigger=npz,
                log_level=log_level
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
                    qc_analyzer.run_qc_analysis(repo, db_type, tgl, kode_qc)
            else:
                logger.info(f"No stragglers found.")
        
        if data_stragglers:
            for sta in data_stragglers:
                kode_qc = sta[0]
                logger.debug(f"Running QC Analysis for straggler: {kode_qc}")
                qc_analyzer.run_qc_analysis(repo, db_type, tgl, kode_qc)
        else:
            logger.info(f"No stragglers found.")
            
        # --- 6. Final Completion Check ---
        # If we specified stations, we *always* exit now.
        if stations:
            logger.info("Specified stations processed. Day complete.")
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