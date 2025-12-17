import logging
import pandas as pd
import re
from typing import Dict, Any

from sqes.services.db_pool import DBPool
from sqes.services.repository import QCRepository

logger = logging.getLogger(__name__)

def _time_to_seconds(time_str):
    # Handle 'NA' case
    if time_str == 'NA':
        return None  # or return 'NA' if you want to output the string

    # Initialize the total seconds variable
    total_seconds = 0
    
    # Use regex to find all time units in the string (e.g., 5m, 12s, 1d)
    time_parts = re.findall(r'(\d+)([smhd])', time_str)

    if not time_parts:  # If no valid time parts were found
        # It's possible time_str is empty or just 0 without unit
        if time_str == '0':
            return 0
        raise ValueError(f"Unrecognized time format: {time_str}")

    # Iterate over the matches
    for value, unit in time_parts:
        value = int(value)
        # Convert based on the unit
        if unit == 's':  # seconds
            total_seconds += value
        elif unit == 'm':  # minutes
            total_seconds += value * 60
        elif unit == 'h':  # hours
            total_seconds += value * 60 * 60
        elif unit == 'd':  # days
            total_seconds += value * 24 * 60 * 60
        else:
            raise ValueError(f"Unrecognized time unit: {unit}")
    return total_seconds

# main
def latency_collector(db_type: str, db_creds: Dict[str, Any], latency_url: str):
    """
    Fetches latency data from URL and append it to 'stations_sensor_latency' table.
    """
    if db_type != 'postgresql':
        logger.error(f"Latency collector is only supported for 'postgresql', not '{db_type}'. Skipping.")
        return

    # 1. Create database connection pool and repository
    try:
        pool = DBPool(
            db_type=db_type,
            host=db_creds.get('host'),
            port=db_creds.get('port'),
            user=db_creds.get('user'),
            password=db_creds.get('password'),
            database=db_creds.get('database'),
            pool_size=db_creds.get('pool_size', 3)
        )
        repo = QCRepository(pool, db_type)
    except Exception as e:
        logger.error(f"Failed to create database connection: {e}", exc_info=True)
        return

    logger.info("--- Collecting latency data")
    try:
        # We can read straight to json instead of dataframe first if we want, but sticking to logic
        df = pd.read_json(latency_url)
    except Exception as e:
        logger.error(f"Failed to fetch or parse latency URL: {e}")
        return

    logger.info(f"--- Processing {len(df)} stations from source")
    
    latency_records = []
    
    for sta_iter in range(len(df)):
        try:
            # Safely access properties
            props = df.features[sta_iter]['properties']
            
            for ch_iter in range(1, 7):
                # Ensure keys exist
                if f'ch{ch_iter}' not in props: 
                    continue

                lat_val = props.get(f'latency{ch_iter}', 'NA')
                latency_seconds = _time_to_seconds(lat_val)
                
                # Handle datetime fields
                # Postgres throws error on empty string for timestamp, so we need None
                datetime_val = props.get('time')
                if not datetime_val:
                    datetime_val = None
                    
                last_time_channel_val = props.get(f'timech{ch_iter}')
                if not last_time_channel_val:
                    last_time_channel_val = None
                
                data = {
                    'net': props.get('net'),
                    'sta': props.get('sta'),
                    'datetime': datetime_val, 
                    'channel': props.get(f'ch{ch_iter}'),
                    'last_time_channel': last_time_channel_val,
                    'latency': latency_seconds,
                    'color_code': props.get(f'color{ch_iter}')
                }
                
                # Simple validation: if net/sta/channel missing, skip?
                if not data['sta'] or not data['channel']:
                    continue

                latency_records.append(data)
                
            if (sta_iter + 1) % 50 == 0:
                logger.debug(f"Processed {sta_iter + 1} stations...")
                
        except Exception as e:
            logger.warning(f"Error processing item {sta_iter}: {e}")
            continue

    logger.info(f"--- Appending {len(latency_records)} records to database")
    if latency_records:
        try:
            repo.bulk_insert_latency_data(latency_records)
            logger.info("--- Latency collection finished successfully")
        except Exception as e:
            logger.error(f"Failed to insert latency records: {e}", exc_info=True)
    else:
        logger.info("--- No records to insert.")