import sys
import requests
import pandas as pd
from sqes_function import Config
import psycopg2
from sqlalchemy import create_engine

# ignore warnings
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

print(f"--- {sys.argv[0]} ---", flush=True)

# connect db
db_config = Config.load_config(section="postgresql") 
engine = psycopg2.connect(**db_config) # type: ignore
engine2 = create_engine('postgresql+psycopg2://sqes:idrip4bmkg@182.16.248.174/sqes')

# load stations db
stations_db = pd.read_sql('select code,latitude,longitude from stations', con=engine)

# get sensor data
print("--------------------------------------------------------------------------------------------")
sensor_df = pd.DataFrame(columns=['code','location','channel','sensor'])
for station in stations_db.code:
    print("Get sensor data: ",station)
    # get data
    url = f'http://202.90.198.40/sismon-wrs/web/detail_slmon/{station}'
    html_ = requests.get(url).content
    df_list = pd.read_html(html_) # type: ignore
    
    # process data
    temp_df = df_list[0].copy()
    temp_df["Station/Channel"] = temp_df["Station/Channel"].str.split(" ")
    temp_df["channel"] = temp_df["Station/Channel"].apply(lambda x: x[1] if not x[1].isnumeric() else x[2])
    temp_df["location"] = temp_df["Station/Channel"].apply(lambda x: x[1] if x[1].isnumeric() else '')
    try:
        temp_df["sensor"] = temp_df["Sensor Type"].apply(lambda x: x.split("-")).apply(lambda x: f"{x[0]}-{x[1]}")
    except:
        temp_df["sensor"] = temp_df["Sensor Type"]
    temp_df["code"] = temp_df["Station/Channel"].apply(lambda x: x[0])
    temp_df["Year"] = temp_df["Sensor Type"].apply(lambda x: x.split("-")[-1])
    temp_df = temp_df[["code","location","channel","sensor"]]

    # concanate data
    sensor_df = pd.concat([sensor_df,temp_df], ignore_index=True)

    # remove unavailable sensor data
    sensor_df = sensor_df[sensor_df.sensor != "xxx"]

    # clear
    del(temp_df)

# push to database
print("--------------------------------------------------------------------------------------------")
print("Pushing data to Database")
sensor_df.to_sql('sensors', con=engine2, if_exists='replace', index=False)

# details
print("--------------------------------------------------------------------------------------------")
print("Details:")
non_colocated = 0
non_colocated_list = []
colocated = 0
colocated_list = []
other = 0
other_list = []
for sensor in stations_db.code:
    tmp = sensor_df[sensor_df['code'] == sensor]
    if len(tmp) == 3:
        non_colocated+=1
        non_colocated_list.append(sensor)
    elif len(tmp) == 6:
        colocated+=1
        colocated_list.append(sensor)
    else:
        other+=1
        other_list.append(sensor)

print("non_colocated",non_colocated)
print(non_colocated_list)
print("colocated",colocated)
print(colocated_list)
print("other",other)
print(other_list)
print("--------------------------------------------------------------------------------------------")
print("process finish")