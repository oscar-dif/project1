import requests, os, configparser
import pandas as pd
import matplotlib.pyplot as plt
import json
import psycopg2 as ps
from sqlalchemy import create_engine

config = configparser.ConfigParser()
CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/data/"
target_dir = data_dir + "/rawfiles/"

conn_string = "postgresql://postgres:lekrum123@localhost/weather_data"
  
db = create_engine(conn_string)
conn = db.connect()

WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"

r = requests.get(WEATHER_URL)

if r.status_code == 200: # If connection is successful (200: http ok)
    json_data = r.json() # Get result in json

dict = json.loads(r.text)

def parameterchoice(choice):
    result = {}
    #result.to_sql("weather_data", postgres_engine, if_exists="replace")
    for n in range(len(dict['timeSeries'])):
        for i in range(len(dict['timeSeries'][n]["parameters"])):
        #print(dict['timeSeries'][0]["parameters"][i])
            if dict['timeSeries'][n]["parameters"][i]['name'] == choice:
                choice_value = dict['timeSeries'][n]["parameters"][i]['values'][0]
        result.update({n : choice_value})
        result[f"{n}"] = choice_value
    return result


weather_data = {
                'temperature': parameterchoice('t'),
                'air_pressure': parameterchoice('msl'),
                'precipitation': parameterchoice('pmean')
            }


# Create DataFrame
df = pd.DataFrame(weather_data)
df.to_sql('weather_data', con=conn, if_exists='replace', index=False)

conn = ps.connect(conn_string)

conn.autocommit = True
cursor = conn.cursor()
  
weather1 = """select * from weather_data;"""
cursor.execute(weather1)
for i in cursor.fetchall():
    print(i)
  
# conn.commit()
conn.close()