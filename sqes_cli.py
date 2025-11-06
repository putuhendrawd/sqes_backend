#!/usr/bin/env python3
import sys
import logging
import argparse
from datetime import datetime
from typing import Optional

# Import from our 'sqes' package
from sqes import __version__
from sqes.services.logging_config import setup_main_logging
from sqes.services.config_loader import load_config
from sqes import workflows

logger = logging.getLogger(__name__)

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
  
  # Run a single day and flush the database
  ./sqes_cli.py --date 20230101 --flush
"""
    )
    
    # --- Date Arguments (Mutually Exclusive) ---
    date_group = parser.add_mutually_exclusive_group(required=True)
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

    # --- Other Optional Flags ---
    parser.add_argument(
        "-n", "--npz",
        action="store_true",
        help="Save PPSD matrix parameters as .npz files"
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
        version=f"%(prog)s {__version__}",  # This will print "sqes_cli.py 1.0.0"
        help="Show the program's version number and exit"
    )

    parser.add_argument(
        "--skip-sensor-update",
        action="store_true",
        help="Skip the automatic update of the 'stations_sensor' table from the sensor_update_url."
    )

    return parser

# --- Main Execution ---
if __name__ == "__main__":
    
    parser = _setup_arguments()
    args = parser.parse_args()
    
    if not args.date and not args.date_range:
        parser.print_help()
        sys.exit(0)

    # 1. Setup Logging
    log_level = setup_main_logging(args.verbose, log_dir="logs")
    
    logger.info(f"--- {sys.argv[0]} Starting ---")
    logger.info(f"Log level set to: {logging.getLevelName(log_level)}")
    
    # 2. Load basic config *first*
    try:
        basic_config = load_config(section='basic')
    except Exception as e:
        logger.critical(f"Failed to load [basic] config: {e}. Exiting.", exc_info=True)
        sys.exit(1)

    # 3. --- NEW: Run Sensor Update ---
    use_db = str(basic_config.get('use_database', 'true')).lower() == 'true'
    
    if use_db and not args.skip_sensor_update:
        logger.info("Running sensor table update...")
        try:
            from sqes.services import data_updater
            
            db_type = basic_config['use_database']
            db_creds = load_config(section=db_type)
            update_url = basic_config.get('sensor_update_url')
            
            if update_url:
                data_updater.update_sensor_table(db_type, db_creds, update_url)
                logger.info("Sensor update complete.")
            else:
                logger.warning("No 'sensor_update_url' in config. Skipping update.")
                
        except Exception as e:
            logger.error(f"Sensor update failed: {e}", exc_info=True)
            # We don't exit; the main processing can still run.
            
    elif not use_db:
        logger.info("Database is disabled, skipping sensor update.")
    else:
        logger.info("User requested --skip-sensor-update.")

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
            npz=args.npz,
            flush=args.flush,
            log_level=log_level,
            basic_config=basic_config
        )
            
    except Exception as e:
        logger.critical(f"A fatal error occurred in the workflow: {e}", exc_info=True)
        sys.exit(1)

    logger.info(f"--- {sys.argv[0]} Finished ---")