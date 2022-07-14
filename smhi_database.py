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


# 1 
#onfig = configparser.ConfigParser()
#config.read(CURR_DIR_PATH + "/config.ini")
#db_pw = config.get("DEV", "psotgres")

def postgres_creator():  # sqlalchemy requires callback-function for connections
  return ps.connect(
      dbname="weather_data",  # name of database
      user="postgres",
      password="postgres",
      host="localhost"
  )
# 3 
WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"
# GET /api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json

r = requests.get(WEATHER_URL)

engine = create_engine(
    url="postgresql+psycopg2://localhost",  # driver identification + dbms api
    creator=postgres_creator  # connection details
)
# 4
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
#####################################################################
# smhidictlist = {"approvedTime":"2022-07-12T20:04:50Z","referenceTime":"2022-07-12T20:00:00Z","geometry":{"type":"Point","coordinates":[[15.990068,57.997072]]},"timeSeries":[{"validTime":"2022-07-12T21:00:00Z","parameters":[{"name":"spp","levelType":"hl","level":0,"unit":"percent","values":[-9]}]}]}
# print(smhidictlist['timeSeries'][0]["parameters"][0]['name'])
# timeSeries --> dict, list--> validtime k&v, parameter: --> dict, lista, dicts
#####################################################################


# 6 
weather_data = {
                "temperature": parameterchoice('t'),
                "air pressure": parameterchoice('pmean'),
                "precipitation": parameterchoice('msl')
            }

#result.to_sql("weather_data", postgres_engine, if_exists="replace")
# 10 11 3 0
# date = validTime                                  -   json_data['timeSeries'][0]['validTime']
# temperature = parameter = t, unit = C             -   json_data['timeSeries'][0]["parameters"][10]['values'][0]
# air pressure = parameter = msl, unit = hPa        -   json_data['timeSeries'][0]["parameters"][11]['values'][0] 
# precipitation = parameter = pmean, unit = mm/h    -   json_data['timeSeries'][0]["parameters"][3]['values'][0]
#def add_weather(C,weather_data):
#    cur = C.cursor()
#    cur.execute(f"INSERT INTO weather_data VALUES ('{weather_data}');")
#    cur.close()


print(weather_data)
#df = pd.DataFrame.to_sql(weather_data)
df = pd.DataFrame.to_sql(name=weather_data, con=engine, if_exists='append', index=True, index_label=None)
engine.execute("SELECT * FROM temperature;").fetchall()
df.to_json(CURR_DIR_PATH + "/data/" + "data.json")

