import logging
import requests
import pandas as pd
from tqdm.auto import tqdm
from typing import Dict, Any
from sqlalchemy.sql.expression import text
from sqlalchemy import create_engine
from io import StringIO
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

def update_sensor_table(db_type: str, db_creds: Dict[str, Any], update_url: str):
    """
    Scrapes the sismon website and updates the 'stations_sensor' table.
    """
    if db_type != 'postgresql':
        logger.error(f"Sensor update is only supported for 'postgresql', not '{db_type}'. Skipping.")
        return

    # 1. Create SQLAlchemy engine from DB credentials
    try:
        # We need to pop 'db_type' as create_engine doesn't expect it
        db_creds.pop('db_type', None) 
        # psycopg2 uses 'dbname' not 'database'
        db_creds['dbname'] = db_creds.pop('database', None) 
        
        user = quote_plus(db_creds['user'])
        password = quote_plus(db_creds['password'])
        host = db_creds['host']
        port = db_creds['port']
        dbname = db_creds['dbname']

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")
    except Exception as e:
        logger.error(f"Failed to create SQLAlchemy engine: {e}", exc_info=True)
        return

    # 2. Get station list from the 'stations' table
    try:
        # with engine.connect() as conn:
        #     raw_conn = conn.connection
        stations_db = pd.read_sql('select code, latitude, longitude from stations', con=engine)
    except Exception as e:
        logger.error(f"Failed to read 'stations' table: {e}. Is table missing?")
        engine.dispose()
        return

    # 3. Loop, scrape, and build the DataFrame
    sensor_df = pd.DataFrame(columns=['code','location','channel','sensor'])
    loop_obj = tqdm(stations_db['code'].tolist(), desc="Updating sensors")
    
    for station in loop_obj:
        try:
            loop_obj.set_description(f"Processing {station}")
            
            # Get data
            url = update_url.format(station_code=station)
            html_ = requests.get(url, timeout=10).text
            df_list = pd.read_html(StringIO(html_))
            
            # Process data
            temp_df = df_list[0].copy()
            temp_df["Station/Channel"] = temp_df["Station/Channel"].str.split(" ")
            temp_df["channel"] = temp_df["Station/Channel"].apply(lambda x: x[1] if not x[1].isnumeric() else x[2])
            temp_df["location"] = temp_df["Station/Channel"].apply(lambda x: x[1] if x[1].isnumeric() else '')
            temp_df["sensor"] = temp_df["Sensor Type"]
            temp_df["code"] = temp_df["Station/Channel"].apply(lambda x: x[0])
            temp_df = temp_df[["code","location","channel","sensor"]]
            
            sensor_df = pd.concat([sensor_df,temp_df], ignore_index=True)
            del(temp_df)
            
        except Exception as e:
            logger.warning(f"Error processing station {station}: {e}. Skipping.")
            continue

    # 4. Clean and write to database
    if sensor_df.empty:
        logger.info("No sensor data found to update.")
        return

    sensor_df = sensor_df[sensor_df.sensor != "xxx"] # Remove unavailable
    sensor_df = sensor_df.drop_duplicates()
    
    logger.info(f"Replacing 'stations_sensor' table with {len(sensor_df)} new entries...")
    
    # --- THIS IS THE NEW LOGIC ---
    try:
        # We use a transaction to safely TRUNCATE and then APPEND.
        # This is much safer than 'if_exists=replace'
        with engine.begin() as conn:
            
            # 1. TRUNCATE the table. This is fast and keeps the table structure.
            logger.debug("Truncating 'stations_sensor' table...")
            conn.execute(text("TRUNCATE TABLE stations_sensor;"))
            
        # 2. Append the new data to the now-empty table.
        logger.debug("Appending new data...")
        # with engine.connect() as conn:
            # raw_conn = conn.connection
        sensor_df.to_sql(
            'stations_sensor',
            con=engine,
            if_exists='append', 
            index=False,
            # method='multi'
        )
        logger.info("Successfully replaced 'stations_sensor' data.")
    
    except Exception as e:
        logger.error(f"Failed to write sensor data to database: {e}", exc_info=True)
    finally:
        engine.dispose()