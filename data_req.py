import requests, os, configparser
import pandas as pd
import matplotlib.pyplot as plt
import json
from sqlalchemy import create_engine
import psycopg2 as ps


CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/raw_data/" + "data.json"
target_dir = CURR_DIR_PATH + "/target_data/" + "data.json"

#behöver en databas med namnet redan finnas? ja.
conn_string = "postgresql://postgres:lekrum123@localhost/weather_data"
db = create_engine(conn_string)
conn = db.connect()
# 3 
WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"
# GET /api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json

r = requests.get(WEATHER_URL)

# 5 
if r.status_code == 200: # If connection is successful (200: http ok)
    json_data = r.json() # Get result in json


# 6     Denna har "w" som ger att skriva över. Ska vi skriva koden så den tittar om ett dokument med det namnet redan finns och gör nånting sen? 
with open(data_dir, "w") as f:
    json.dump(json_data, f)


dict = json.loads(r.text)



def parameterchoice(choice):
    result = {} # empty list
    for n in range(len(dict['timeSeries'])):
        for i in range(len(dict['timeSeries'][n]["parameters"])):            
            if dict['timeSeries'][n]["parameters"][i]['name'] == choice:
                choice_value = dict['timeSeries'][n]["parameters"][i]['values'][0]  
        result[f"{n}"] = choice_value
    return result


weather_data = {
                "temperature": parameterchoice('t'),
                "air pressure": parameterchoice('pmean'),
                "precipitation": parameterchoice('msl')
            }
              

# jag har ändrat här
df = pd.DataFrame(weather_data)
df.to_json(target_dir)

# Create DataFrame

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