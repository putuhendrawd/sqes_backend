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
print("--------------------------------------------------------------------------------------------")
# connect db
db_config = Config.load_config(section="postgresql") 
engine = psycopg2.connect(**db_config) # type: ignore
engine2 = create_engine('postgresql+psycopg2://sqes:idrip4bmkg@182.16.248.174/sqes')

# load stations_visit db
stations_visit_db = pd.read_sql('select * from stations_visit', con=engine)

# processing
visit_list = []
for i, code in stations_visit_db.iterrows():
    if code.visit_year is None:
        print(code.code, 0)
        visit_list.append(0)
    else:
        count = len(code.visit_year.split(","))
        visit_list.append(count)
        print(code.code, count)
    print("--------------------------------------------------------------------------------------------")
stations_visit_db.visit_count = visit_list

# push data to db
print("Pushing data to Database")
stations_visit_db.to_sql('stations_visit', con=engine2, if_exists='replace', index=False)

print("--------------------------------------------------------------------------------------------")
print("process finish")