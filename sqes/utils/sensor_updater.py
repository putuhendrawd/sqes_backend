import logging
import requests
import pandas as pd
from typing import Dict, Any
from io import StringIO

from sqes.services.db_pool import DBPool
from sqes.services.repository import QCRepository

logger = logging.getLogger(__name__)

def update_sensor_table(db_type: str, db_creds: Dict[str, Any], update_url: str):
    """
    Scrapes the sismon website and updates the 'stations_sensor' table.
    """
    if db_type != 'postgresql':
        logger.error(f"Sensor update is only supported for 'postgresql', not '{db_type}'. Skipping.")
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

    # 2. Get station list from the 'stations' table
    try:
        stations_result = repo.get_all_stations_basic()
        # Extract just the station codes
        station_codes = [row[0] for row in stations_result]
            
    except Exception as e:
        logger.error(f"Failed to read 'stations' table: {e}. Is table missing?", exc_info=True)
        return

    # 3. Loop, scrape, and build the DataFrame
    sensor_df = pd.DataFrame(columns=['code','location','channel','sensor'])
    total_stations = len(station_codes)
    logger.info(f"Starting sensor metadata update for {total_stations} stations...")
    
    success_count = 0
    error_count = 0
    
    for idx, station in enumerate(station_codes, 1):
        try:
            # Log progress every 10 stations
            if idx % 50 == 0 or idx == 1:
                logger.info(f"Progress: {idx}/{total_stations} stations processed ({success_count} successful, {error_count} errors)")
            
            url = update_url.format(station_code=station)
            html_ = requests.get(url, timeout=10).text
            df_list = pd.read_html(StringIO(html_))
            
            temp_df = df_list[0].copy()
            temp_df = temp_df.dropna()
            temp_df["Station/Channel"] = temp_df["Station/Channel"].str.split(" ")
            temp_df["channel"] = temp_df["Station/Channel"].apply(lambda x: x[1] if not x[1].isnumeric() else x[2])
            temp_df["location"] = temp_df["Station/Channel"].apply(lambda x: x[1] if x[1].isnumeric() else '')
            temp_df["sensor"] = temp_df["Sensor Type"]
            temp_df["code"] = temp_df["Station/Channel"].apply(lambda x: x[0])
            temp_df = temp_df[["code","location","channel","sensor"]]
            
            sensor_df = pd.concat([sensor_df,temp_df], ignore_index=True)
            del(temp_df)
            success_count += 1
            
        except Exception as e:
            logger.warning(f"Error processing station {station}: {e}. Skipping.")
            error_count += 1
            continue
    
    logger.info(f"Sensor scraping complete: {success_count} successful, {error_count} errors")

    # 4. Clean and write to database
    if sensor_df.empty:
        logger.info("No sensor data found to update.")
        return

    sensor_df = sensor_df[sensor_df.sensor != "xxx"] # Remove unavailable
    sensor_df = sensor_df.drop_duplicates()
    
    # Get a list of the unique station codes we just scraped
    unique_codes_scraped = sensor_df['code'].unique().tolist()
    
    if not unique_codes_scraped:
        logger.info("No valid sensor data was scraped. Database is unchanged.")
        return

    logger.info(f"Updating {len(unique_codes_scraped)} stations in 'stations_sensor' table...")
    
    try:
        # Delete old entries for the scraped stations
        logger.debug(f"Deleting old entries for {len(unique_codes_scraped)} stations...")
        repo.delete_sensor_data_for_stations(unique_codes_scraped)
        
        # Convert DataFrame to list of dictionaries for bulk insert
        sensor_records = sensor_df.to_dict('records')
        
        # Bulk insert the new data
        logger.debug(f"Inserting {len(sensor_records)} new sensor entries...")
        repo.bulk_insert_sensor_data(sensor_records)
        
        logger.info(f"Successfully updated sensor data for {len(unique_codes_scraped)} stations.")
            
    except Exception as e:
        logger.error(f"Failed to write sensor data to database: {e}", exc_info=True)