import logging
import requests
import pandas as pd
from tqdm.auto import tqdm
from typing import Dict, Any
from sqlalchemy import create_engine, text
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

    # 1. Create SQLAlchemy engine
    try:
        db_creds.pop('db_type', None) 
        db_creds['dbname'] = db_creds.pop('database', None) 
        
        user = quote_plus(db_creds['user'])
        password = quote_plus(db_creds['password'])
        host = db_creds['host']
        port = db_creds['port']
        dbname = db_creds['dbname']
        
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        
        engine = create_engine(url)
    except Exception as e:
        logger.error(f"Failed to create SQLAlchemy engine: {e}", exc_info=True)
        return

    # 2. Get station list from the 'stations' table
    try:
        stations_db = pd.read_sql('select code, latitude, longitude from stations', con=engine)
            
    except Exception as e:
        logger.error(f"Failed to read 'stations' table: {e}. Is table missing?", exc_info=True)
        engine.dispose()
        return

    # 3. Loop, scrape, and build the DataFrame
    sensor_df = pd.DataFrame(columns=['code','location','channel','sensor'])
    loop_obj = tqdm(stations_db['code'].tolist(), desc="Updating sensors")
    
    for station in loop_obj:
        try:
            loop_obj.set_description(f"Processing {station}")
            
            url = update_url.format(station_code=station)
            html_ = requests.get(url, timeout=10).text
            df_list = pd.read_html(StringIO(html_))
            
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
        engine.dispose()
        return

    sensor_df = sensor_df[sensor_df.sensor != "xxx"] # Remove unavailable
    sensor_df = sensor_df.drop_duplicates()
    
    # --- THIS IS THE NEW LOGIC ---
    
    # Get a list of the unique station codes we just scraped
    unique_codes_scraped = sensor_df['code'].unique().tolist()
    
    if not unique_codes_scraped:
        logger.info("No valid sensor data was scraped. Database is unchanged.")
        engine.dispose()
        return

    logger.info(f"Updating {len(unique_codes_scraped)} stations in 'stations_sensor' table...")
    
    try:
        with engine.begin() as conn:
            
            # 1. Create named parameters: [":p1", ":p2", ":p3"]
            named_params = [f":p{i}" for i in range(len(unique_codes_scraped))]
            
            # 2. Create the placeholder string: "(:p1, :p2, :p3)"
            placeholders = f"({', '.join(named_params)})"
            
            # 3. Create the SQL DELETE statement using the named params
            sql_delete_query = text(
                f"DELETE FROM stations_sensor WHERE code IN {placeholders}"
            )
            
            # 4. Create the parameters dictionary: {"p1": "AAFM", "p2": "BBJI", ...}
            params_dict = {f"p{i}": code for i, code in enumerate(unique_codes_scraped)}
            
            # 5. Execute the DELETE with the dictionary
            logger.debug(f"Deleting old entries for {len(unique_codes_scraped)} stations...")
            conn.execute(sql_delete_query, params_dict)
            
            # 6. Append the new data to the table.
            logger.debug(f"Appending {len(sensor_df)} new sensor entries...")
            sensor_df.to_sql(
                'stations_sensor',
                con=conn,
                if_exists='append', 
                index=False,
                method='multi' # Use 'multi' for stable inserts
            )
        
        logger.info(f"Successfully updated sensor data for {len(unique_codes_scraped)} stations.")
            
    except Exception as e:
        logger.error(f"Failed to write sensor data to database: {e}", exc_info=True)
    finally:
        engine.dispose()