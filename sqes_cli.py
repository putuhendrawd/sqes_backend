#!/usr/bin/env python3
import sys
import logging
import argparse
import signal
from datetime import datetime

from sqes import __version__
from sqes.services.logging_config import setup_main_logging
from sqes.services.config_loader import load_config
from sqes import workflows

logger = logging.getLogger(__name__)

def _handle_termination_signal(signum, frame):
    """Handle termination signals (SIGTERM, SIGINT) and log before exiting."""
    signal_names = {
        signal.SIGTERM: "SIGTERM",
        signal.SIGINT: "SIGINT (Ctrl+C)",
        signal.SIGHUP: "SIGHUP"
    }
    signal_name = signal_names.get(signum, f"signal {signum}")
    
    logger.warning(f"")
    logger.warning(f"{'='*70}")
    logger.warning(f"⚠️  Process received {signal_name} - Terminating gracefully")
    logger.warning(f"{'='*70}")
    logger.warning(f"")
    
    # Flush logs before exit
    logging.shutdown()
    
    sys.exit(128 + signum)  # Standard exit code for signal termination

def _setup_arguments():
    """Configures command-line arguments using argparse."""
    parser = argparse.ArgumentParser(
        description="SQES: Seismic Quality Evaluation System.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all stations for a single day (Warning level)
  ./sqes_cli.py --date 20230101

  # Run with INFO level logging
  ./sqes_cli.py --date 20230101 -v

  # Run with DEBUG level logging
  ./sqes_cli.py --date 20230101 -vv

  # Run specific stations with INFO level
  ./sqes_cli.py --date-range 20230101 20230103 -s BBJI GSI -v
  
  # Run specific network with INFO level
  ./sqes_cli.py --date 20230101 -n IA -v

  # Run specific stations and networks with INFO level (will check if stations are in networks)
  ./sqes_cli.py --date 20230101 -s BBJI GSI -n IA II -v

  # Run a single day and flush the database
  ./sqes_cli.py --date 20230101 --flush

  # Run only station update (automatically runs sensor update too)
  ./sqes_cli.py --station-update
  
  # Run a single day with station and sensor update
  ./sqes_cli.py --date 20230101 --station-update
  
  # Run only sensor update (no date processing)
  ./sqes_cli.py --sensor-update
  
  # Run single day with mseed and npz saved
  ./sqes_cli.py --date 20230101 --mseed --ppsd
"""
    )
    
    # --- Date Arguments (Mutually Exclusive) ---
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "-d", "--date",
        metavar="YYYYMMDD",
        type=str,
        default=None,
        help="Process a single date."
    )
    date_group.add_argument(
        "-r", "--date-range",
        metavar=("START", "END"),
        nargs=2,
        type=str,
        default=None,
        help="Process a date range (YYYYMMDD YYYYMMDD), inclusive."
    )

    # --- Station Argument (Optional) ---
    parser.add_argument(
        "-s", "--station", "--stations",
        dest="stations", 
        metavar="STA",
        nargs='+',  # This accepts one or more space-separated values
        type=str,
        default=None, # Will be None if not provided
        help="Optional: Process only these specific station codes. (Default: all)"
    )

    # --- Network Argument (Optional) ---
    parser.add_argument(
        "-n", "--network",
        dest="network",
        metavar="NET",
        nargs='+',
        type=str,
        default=None,
        help="Optional: Process only stations belonging to these networks."
    )

    # --- Other Optional Flags ---
    parser.add_argument(
        "--ppsd",
        action="store_true",
        help="Save PPSD matrix parameters as .npz files"
    )
    parser.add_argument(
        "--mseed",
        action="store_true",
        help="Save the downloaded waveform data as MiniSEED files."
    )
    parser.add_argument(
        "-f", "--flush",
        action="store_true",
        help="Flush existing data *only* when using --date (not --date-range)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="count",  # Use 'count' to sum up the -v flags
        default=0,
        help="Increase logging verbosity (default: WARNING, -v: INFO, -vv: DEBUG)"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}", 
        help="Show the program's version number and exit"
    )

    parser.add_argument(
        "--station-update",
        action="store_true",
        help="Perform an automatic update of the 'stations' table from the station_update_url. (Default: skip update). Automatically runs --sensor-update afterwards."
    )

    parser.add_argument(
        "--sensor-update",
        action="store_true",
        help="Perform an automatic update of the 'stations_sensor' table from the sensor_update_url. (Default: skip update)"
    )

    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Check all configuration, test connection, and exit."
    )

    parser.add_argument(
        "--latency-collector",
        action="store_true",
        help="Collect latency data and append to 'stations_sensor_latency' table."
    )

    return parser

# --- Main Execution ---
if __name__ == "__main__":
    
    parser = _setup_arguments()
    args = parser.parse_args()
    
    if not args.date and not args.date_range and not args.check_config and not args.sensor_update and not args.station_update and not args.latency_collector:
        parser.print_help()
        sys.exit(0)

    # 1. Setup Logging
    log_date_str = None
    if args.date:
        log_date_str = args.date
    elif args.date_range:
        log_date_str = args.date_range[0] # Use the start date
    elif args.check_config:
        log_date_str = f"config_{datetime.now().strftime('%Y%m%d')}"
    elif args.latency_collector:
        log_date_str = f"latency_{datetime.now().strftime('%Y%m%d_%H%M')}"
    else:
        # Fallback for --check-config or if no date is given
        log_date_str = datetime.now().strftime('%Y%m%d') 

    if args.check_config:
        log_level, log_file_path = setup_main_logging(args.verbose+1, log_date_str, log_dir="logs/log")   
    else:
        log_level, log_file_path = setup_main_logging(args.verbose, log_date_str, log_dir="logs/log")
    
    # Register signal handlers after logging is set up
    signal.signal(signal.SIGTERM, _handle_termination_signal)
    signal.signal(signal.SIGINT, _handle_termination_signal)
    try:
        signal.signal(signal.SIGHUP, _handle_termination_signal)  # Not available on Windows
    except AttributeError:
        pass  # SIGHUP not available on Windows
    
    # Log the start banner and the arguments immediately
    logger.info(f"--- {sys.argv[0]} Starting ---")
    logger.info(f"Arguments: {vars(args)}")
    logger.info(f"Log level set to: {logging.getLevelName(log_level)}")
    
    if args.check_config:
        logger.info("Running configuration and connection check...")
        try:
            from sqes.services.health_check import check_configurations
            all_ok = check_configurations()
            
            if all_ok:
                logger.info("--- ✅ All checks passed ---")
                sys.exit(0)
            else:
                logger.error("--- ❌ One or more checks FAILED ---")
                sys.exit(1)
        except Exception as e:
            logger.critical(f"A fatal error occurred during config check: {e}", exc_info=True)
            sys.exit(1)

    logger.info(f"--- {sys.argv[0]} Starting ---")
    logger.info(f"Log level set to: {logging.getLevelName(log_level)}")
    
    # 2. Load basic config *first*
    try:
        basic_config = load_config(section='basic')
    except Exception as e:
        logger.critical(f"Failed to load [basic] config: {e}. Exiting.", exc_info=True)
        sys.exit(1)

    # 3. Run station update (if requested, automatically runs sensor update too)
    use_db_config_value = str(basic_config.get('use_database', 'true')).lower()
    use_db = use_db_config_value not in ['false', 'no', '0']
    
    if use_db and args.station_update:
        logger.info("Running station table update...")
        try:
            from sqes.utils import station_updater
            
            db_type = basic_config['use_database']
            db_creds = load_config(section=db_type)
            update_url = basic_config.get('station_update_url')
            
            if update_url:
                station_updater.update_station_table(db_type, db_creds, update_url)
                logger.info("Station update complete.")
            else:
                logger.warning("No 'station_update_url' in config. Skipping station update.")
                
        except Exception as e:
            logger.error(f"Station update failed: {e}", exc_info=True)
            sys.exit(1)
        
        # Automatically run sensor update after station update
        logger.info("Automatically running sensor update after station update...")
        args.sensor_update = True
    
    # 4. Run sensor update
    if use_db and args.sensor_update:
        logger.info("Running sensor table update...")
        try:
            from sqes.utils import sensor_updater
            
            db_type = basic_config['use_database']
            db_creds = load_config(section=db_type)
            update_url = basic_config.get('sensor_update_url')
            
            if update_url:
                sensor_updater.update_sensor_table(db_type, db_creds, update_url)
                logger.info("Sensor update complete.")
            else:
                logger.warning("No 'sensor_update_url' in config. Skipping update.")
                
        except Exception as e:
            logger.error(f"Sensor update failed: {e}", exc_info=True)
            sys.exit(1)
            
    elif not use_db:
        logger.info("Database is disabled, skipping sensor/station updates.")
    elif not args.sensor_update and not args.station_update:
        logger.info("--sensor-update or --station-update not specified, skipping updates.")

    # 4. Run latency collector
    if use_db and args.latency_collector:
        logger.info("Running latency collector...")
        try:
            from sqes.utils import latency_collector
            
            db_type = basic_config['use_database']
            db_creds = load_config(section=db_type)
            latency_url = basic_config.get('latency_update_url')
            
            if latency_url:
                latency_collector.latency_collector(db_type, db_creds, latency_url)
            else:
                logger.warning("No 'latency_update_url' in config. Skipping latency collection.")
                
        except Exception as e:
            logger.error(f"Latency collection failed: {e}", exc_info=True)
            sys.exit(1)

    # If only station/sensor/latency update was requested (no date processing), exit here
    if (args.sensor_update or args.station_update or args.latency_collector) and not args.date and not args.date_range:
        logger.info("Update(s) completed. No date processing requested. Exiting.")
        logger.info(f"--- {sys.argv[0]} Finished ---")
        sys.exit(0)

    # 4. Validate and Parse Arguments
    if args.flush and args.date_range:
        logger.error("--flush can only be used with --date, not --date-range.")
        sys.exit(1)
        
    start_date_str = ""
    end_date_str = ""
    
    if args.date:
        start_date_str = args.date
        end_date_str = args.date
        logger.info(f"Processing single date: {start_date_str}")
    elif args.date_range:
        start_date_str = args.date_range[0]
        end_date_str = args.date_range[1]
        logger.info(f"Processing date range: {start_date_str} to {end_date_str}")

    if args.stations:
        logger.info(f"Filtering for stations: {args.stations}")
    else:
        logger.info("Processing all stations.")

    # 3. Dispatch to the main workflow
    try:
        workflows.run_processing_workflow(
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            stations=args.stations,
            network=args.network,
            ppsd=args.ppsd,
            mseed=args.mseed,
            flush=args.flush,
            log_level=log_level,
            log_file_path=log_file_path,
            basic_config=basic_config
        )
            
    except Exception as e:
        logger.critical(f"A fatal error occurred in the workflow: {e}", exc_info=True)
        sys.exit(1)

    logger.info(f"--- {sys.argv[0]} Finished ---")