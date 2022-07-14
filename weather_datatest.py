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

def postgres_creator():  # sqlalchemy requires callback-function for connections
  return ps.connect(
      dbname="weather_data",  # name of database
      user="postgres",
      password="lekrum123",
      host="localhost"
  )

WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"

r = requests.get(WEATHER_URL)

engine = create_engine(
    url="postgresql+psycopg2://localhost",  # driver identification + dbms api
    creator=postgres_creator  # connection details
)

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

engine.execute(f"CREATE TABLE IF NOT EXISTS weather_data(temperature float, air_pressure float, precipipation float);")
#weather_data_db = """CREATE TABLE IF NOT EXISTS weather_data(temperature float, air_pressure float, precipipation float);"""
df = pd.DataFrame(weather_data, columns= ['temperature','air_pressure', 'precipitation'])
print (df)

engine.execute(f"INSERT INTO weather_data ({df.columns}) VALUES({weather_data});")
#df = pd.DataFrame.from_dict(weather_data)
#df1 = pd.DataFrame.to_sql(self=df, name=weather_data, con=engine, if_exists='append', index=True, index_label=None, method=None)

#df.to_sql('weather_data', con=engine, if_exists='append', index = True, index_label=None, method=None)
engine.execute("SELECT * FROM weather_data;").fetchall()
df.to_json(CURR_DIR_PATH + "/data/" + "data.json")