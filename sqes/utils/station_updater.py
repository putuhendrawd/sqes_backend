import logging
import json
import re
import requests
import pandas as pd
from typing import Dict, Any

from sqes.services.db_pool import DBPool
from sqes.services.repository import QCRepository

logger = logging.getLogger(__name__)

def update_station_table(db_type: str, db_creds: Dict[str, Any], update_url: str):
    """
    Fetches station data from URL and updates the 'stations' table.
    """
    if db_type != 'postgresql':
        logger.error(f"Station update is only supported for 'postgresql', not '{db_type}'. Skipping.")
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
        stations_db = pd.DataFrame(stations_result, columns=[
            'code', 'latitude', 'longitude', 'network', 'province', 
            'location', 'upt', 'digitizer_type', 'communication_type'
        ])
            
    except Exception as e:
        logger.error(f"Failed to read 'stations' table: {e}. Is table missing?", exc_info=True)
        return

    # 3. Get data from update url
    try:
        html_ = requests.get(update_url, timeout=10).content
        json_data = json.loads(html_)
        json_data = json_data['features']

        # list sta in json_data 
        json_stations = []
        for item in json_data:
            if 'properties' in item and 'sta' in item['properties']:
                json_stations.append(item['properties']['sta'])
        
    except Exception as e:
        logger.error(f"Failed to fetch data from update URL: {e}", exc_info=True)
        return

    # 4. differentiate stations_db and json_stations
    stations_to_add = list(set(json_stations) - set(stations_db['code'].tolist()))
    stations_to_update = list(set(stations_db['code'].tolist()) & set(json_stations))

    # 5. Add new stations
    insert_success = 0
    insert_skipped = 0
    insert_error = 0
    
    try:
        if stations_to_add:
            total_to_add = len(stations_to_add)
            logger.info(f"Inserting {total_to_add} new stations...")
            
            for idx, sta in enumerate(stations_to_add, 1):
                try:
                    # Log progress every 10 stations or on first
                    if idx % 10 == 0 or idx == 1:
                        logger.info(f"Progress: {idx}/{total_to_add} new stations processed ({insert_success} inserted, {insert_skipped} skipped)")
                    
                    station_data = next((item for item in json_data if item['properties']['sta'] == sta), None)
                    if station_data:
                        longitude = station_data['geometry'].get('coordinates', [None, None])[0]
                        latitude = station_data['geometry'].get('coordinates', [None, None])[1]
                        network = station_data['properties'].get('net')
                        province = station_data['properties'].get('provin')
                        location = station_data['properties'].get('location')
                        upt = station_data['properties'].get('uptbmkg')
                        digitizer_type = station_data['properties'].get('merkdgtz')
                        match = re.search(r'(?:19|20)\d{2}-(.*)', digitizer_type) if digitizer_type else None
                        communication_type = match.group(1).strip() if match else None
                        
                        if latitude is not None and longitude is not None:
                            logger.debug(f"Inserting new station: {sta}")
                            repo.insert_station({
                                'code': sta,
                                'network': network,
                                'latitude': latitude,
                                'longitude': longitude,
                                'province': province,
                                'location': location,
                                'upt': upt,
                                'digitizer_type': digitizer_type,
                                'communication_type': communication_type
                            })
                            insert_success += 1
                        else:
                            logger.warning(f"Skipping insert for {sta} due to missing coordinates")
                            insert_skipped += 1
                    else:
                        logger.warning(f"Could not find data for new station {sta} in JSON")
                        insert_skipped += 1
                        
                except Exception as e:
                    logger.error(f"Error inserting station {sta}: {e}")
                    insert_error += 1
                    continue
            
            logger.info(f"Station insertion complete: {insert_success} inserted, {insert_skipped} skipped, {insert_error} errors")
        else:
            logger.info("No new stations to insert")
    except Exception as e:
        logger.error(f"Failed to insert stations: {e}", exc_info=True)
        return

    # 6. update stations
    try:
        if stations_to_update:
            total_to_check = len(stations_to_update)
            logger.info(f"Checking {total_to_check} existing stations for updates...")
            # Make 'code' the index for faster lookups (.loc)
            stations_db.set_index('code', inplace=True)
            
            update_count = 0
            no_change_count = 0
            
            for idx, sta in enumerate(stations_to_update, 1):
                # Log progress every 50 stations or on first
                if idx % 50 == 0 or idx == 1:
                    logger.info(f"Progress: {idx}/{total_to_check} stations checked ({update_count} updated, {no_change_count} unchanged)")
                
                # Get the new data from the JSON file
                json_station_data = next((item for item in json_data if item['properties']['sta'] == sta), None)
                if not json_station_data:
                    logger.warning(f"Skipping update for {sta}, not found in JSON source")
                    continue

                # Get the existing data from the database DataFrame
                db_station_row = stations_db.loc[sta]

                # --- Extract new values from JSON ---
                # --- Create a dictionary of fields to update ---
                updates = {}

                # Check coordinates (longitude, latitude)
                new_longitude = json_station_data['geometry'].get('coordinates', [None, None])[0]
                new_latitude = json_station_data['geometry'].get('coordinates', [None, None])[1]
                
                if new_longitude is not None and new_longitude != db_station_row.get('longitude'):
                    updates['longitude'] = new_longitude
                if new_latitude is not None and new_latitude != db_station_row.get('latitude'):
                    updates['latitude'] = new_latitude

                # Check other properties
                field_map = {
                    'net': 'network',
                    'provin': 'province',
                    'location': 'location',
                    'uptbmkg': 'upt',
                    'merkdgtz': 'digitizer_type'
                }

                for json_key, db_column in field_map.items():
                    new_value = json_station_data['properties'].get(json_key)
                    # Check if the new value is different from the old one in the DB
                    if new_value != db_station_row.get(db_column):
                        updates[db_column] = new_value

                new_digitizer = json_station_data['properties'].get('merkdgtz')
                match = re.search(r'(?:19|20)\d{2}-(.*)', new_digitizer) if new_digitizer else None
                new_comm_type = match.group(1).strip() if match else None
                if new_comm_type != db_station_row.get('communication_type'):
                    updates['communication_type'] = new_comm_type

                    
                # --- If there are changes, execute the UPDATE statement ---
                if updates:
                    logger.debug(f"Updating station {sta} with data: {updates}")
                    repo.update_station(sta, updates)
                    update_count += 1
                else:
                    logger.debug(f"No updates needed for {sta}")
                    no_change_count += 1

            logger.info(f"Station update check complete: {update_count} updated, {no_change_count} unchanged")
        else:
            logger.info("No existing stations to check for updates")
        logger.info("Station data synchronization complete")
    except Exception as e:
        logger.error(f"Failed to update stations: {e}", exc_info=True)
        return

    # 7. Sync stations data to other tables
    logger.info("Synchronizing station codes across related tables...")
    try:
        # Get station codes from all tables
        stations_codes = [row[0] for row in repo.get_station_codes_from_table('stations')]
        dominant_codes = [row[0] for row in repo.get_station_codes_from_table('stations_dominant_data_quality')]
        site_quality_codes = [row[0] for row in repo.get_station_codes_from_table('stations_site_quality')]
        visit_codes = [row[0] for row in repo.get_station_codes_from_table('stations_visit')]

        logger.info(f"Stations in main table: {len(stations_codes)} (unique: {len(set(stations_codes))})")
        
        # Find missing stations
        stations_not_in_dominant = list(set(stations_codes) - set(dominant_codes))
        stations_not_in_site_quality = list(set(stations_codes) - set(site_quality_codes))
        stations_not_in_visit = list(set(stations_codes) - set(visit_codes))

        logger.info(f"Stations in dominant_data_quality: {len(dominant_codes)}, missing: {len(stations_not_in_dominant)}")
        logger.info(f"Stations in site_quality: {len(site_quality_codes)}, missing: {len(stations_not_in_site_quality)}")
        logger.info(f"Stations in visit: {len(visit_codes)}, missing: {len(stations_not_in_visit)}")

        # Sync Process
        total_sync_count = 0
        
        # Sync stations_dominant_data_quality
        if stations_not_in_dominant:
            logger.info(f"Syncing {len(stations_not_in_dominant)} stations to dominant_data_quality table...")
            for idx, sta in enumerate(stations_not_in_dominant, 1):
                if idx % 50 == 0 or idx == 1:
                    logger.info(f"Progress: {idx}/{len(stations_not_in_dominant)} stations synced to dominant_data_quality")
                logger.debug(f"Inserting {sta} into stations_dominant_data_quality")
                repo.insert_station_into_table('stations_dominant_data_quality', sta)
            total_sync_count += len(stations_not_in_dominant)
            logger.info(f"Completed syncing {len(stations_not_in_dominant)} stations to dominant_data_quality")
        else:
            logger.info("No stations to sync to dominant_data_quality")

        # Sync stations_site_quality
        if stations_not_in_site_quality:
            logger.info(f"Syncing {len(stations_not_in_site_quality)} stations to site_quality table...")
            for idx, sta in enumerate(stations_not_in_site_quality, 1):
                if idx % 50 == 0 or idx == 1:
                    logger.info(f"Progress: {idx}/{len(stations_not_in_site_quality)} stations synced to site_quality")
                logger.debug(f"Inserting {sta} into stations_site_quality")
                repo.insert_station_into_table('stations_site_quality', sta)
            total_sync_count += len(stations_not_in_site_quality)
            logger.info(f"Completed syncing {len(stations_not_in_site_quality)} stations to site_quality")
        else:
            logger.info("No stations to sync to site_quality")

        # Sync stations_visit
        if stations_not_in_visit:
            logger.info(f"Syncing {len(stations_not_in_visit)} stations to visit table...")
            for idx, sta in enumerate(stations_not_in_visit, 1):
                if idx % 50 == 0 or idx == 1:
                    logger.info(f"Progress: {idx}/{len(stations_not_in_visit)} stations synced to visit")
                logger.debug(f"Inserting {sta} into stations_visit")
                repo.insert_station_into_table('stations_visit', sta)
            total_sync_count += len(stations_not_in_visit)
            logger.info(f"Completed syncing {len(stations_not_in_visit)} stations to visit")
        else:
            logger.info("No stations to sync to visit")

        logger.info(f"Table synchronization complete: {total_sync_count} total records synced across related tables")
    except Exception as e:
        logger.error(f"Failed to sync stations across tables: {e}", exc_info=True)
