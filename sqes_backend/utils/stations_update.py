import sys
from sqes_function import Config
import psycopg2
import pandas as pd
from obspy import read_inventory

# ignore warnings
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

print(f"--- {sys.argv[0]} ---", flush=True)

# connect db
db_config = Config.load_config(section="postgresql") 
engine = psycopg2.connect(**db_config) # type: ignore

# load db
stations_db = pd.read_sql('select code,latitude,longitude from stations', con=engine)

# update db
sql="""UPDATE stations SET latitude=%s, longitude=%s WHERE code=%s"""
rowcount=0
for code in stations_db.code:
    print(f"processing: {code}", flush=True)
    # take data from station xml
    inv = read_inventory(f"https://geof.bmkg.go.id/fdsnws/station/1/query?station={code}&level=response&nodata=404")
    dict_xml = {
    'code': inv[0].stations[0].code,
    'latitude': inv[0].stations[0].latitude,
    'longitude' : inv[0].stations[0].longitude
    }
    # processing diff
    db_lat = stations_db[stations_db.code==code].latitude.values[0]
    db_lon = stations_db[stations_db.code==code].longitude.values[0]
    diff_lat=db_lat-dict_xml['latitude']
    diff_lon=db_lon-dict_xml['longitude']
    print(f"latitude_def : {db_lat: >10} | latitude_xml : {dict_xml['latitude']: >10} | diff: {diff_lat}", flush=True)
    print(f"longitude_def: {db_lon: >10} | longitude_xml: {dict_xml['longitude']: >10} | diff: {diff_lon}", flush=True)
    
    # update db
    with engine.cursor() as cur:
        cur.execute(sql,(dict_xml['latitude'],dict_xml['longitude'],code))
        rowcount=cur.rowcount
    engine.commit()
    print(f"updated db row {rowcount}", flush=True)
    
    print(f"end of {code}", flush=True)
    print("--------------------------------------------------------------------------------------------", flush=True)
print("process finish", flush=True)