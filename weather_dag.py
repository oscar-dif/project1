from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedeltas
import requests, os, configparser
import pandas as pd
import matplotlib.pyplot as plt
import json
from sqlalchemy import *
import psycopg2 as ps

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/raw_data/" + "data.json"
target_dir = CURR_DIR_PATH + "/target_data/" + "data.json"

# Initializes configuration from the config.ini file
config = configparser.ConfigParser()
config.read(CURR_DIR_PATH + "/config.ini")

# Fetches the api key from your config.ini file
API_KEY = config.get("DEV", "API_KEY")
USER = config.get("DEV", "USER")


conn_string = f"postgresql://{USER}:{API_KEY}@127.0.0.1/weather_data"
#conn_string = f"postgresql://postgres:{API_KEY}@localhost/weather_data"
db = create_engine(conn_string)
conn = db.connect()
WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"

#******************************************************************************************
def request():
    r = requests.get(WEATHER_URL)
    return r

def rawdata(): #gets api data and saves in json file
    r = request()
    if r.status_code == 200: # If connection is successful (200: http ok)
        json_data = r.json() # Get result in json
    with open(data_dir, "w") as f:
        json.dump(json_data, f)





def parameterchoice(choice): #searches for data in api
    r = request()
    dict = json.loads(r.text)
    result = {} 
    for n in range(len(dict['timeSeries'])):
        for i in range(len(dict['timeSeries'][n]["parameters"])):            
            if dict['timeSeries'][n]["parameters"][i]['name'] == choice:
                choice_value = dict['timeSeries'][n]["parameters"][i]['values'][0]  
        result[f"{n}"] = choice_value
    return result






def create_harmony_dict(): #creates dict from selected data
    weather_data = {
                    "temperature": parameterchoice('t'),
                    "air pressure": parameterchoice('pmean'),
                    "precipitation": parameterchoice('msl')
                }
    return weather_data

def save_harmony_data(): #saves harmonized data in json
    df = pd.DataFrame(create_harmony_dict()) 
    df.to_json(target_dir) 





def sql_transfer():
    df = pd.DataFrame(create_harmony_dict()) 
    df.to_sql('weather_data', conn, if_exists='replace', index=False)
    conn.autocommit = True 
    weather1 = db.execute(text("select * from weather_data;"))
    for i in weather1:
        print(i)




#####*****************************DAG*********************************************
with DAG("raw_data", start_date=datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=1), catchup=False) as dag:

        rawdata = PythonOperator(
            task_id="rawdata",
            python_callable=rawdata
        )



#-----------------------------------------------------------------------------#

        parameterchoice = PythonOperator(
            task_id = "choice_of_data",
            python_callable=parameterchoice('t')
        )
        
        create_harmony_dict = PythonOperator(
            task_id = "harmonized",
            python_callable=create_harmony_dict
        )
        
        save_harmony_data = PythonOperator(
            task_id = "save_harmony_data",
            python_callable=save_harmony_data
        )

        sql_transfer = PythonOperator(
            task_id = "data_to_postgres_db",
            python_callable=sql_transfer
        )



        #[training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]
        [rawdata] >> [parameterchoice] >> [create_harmony_dict] >> [save_harmony_data] >> [sql_transfer] 