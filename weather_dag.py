from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
import requests, os
import pandas as pd
import matplotlib.pyplot as plt
import json
from sqlalchemy import create_engine
import psycopg2 as ps

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/raw_data/" + "data.json"
target_dir = CURR_DIR_PATH + "/target_data/" + "data.json"

# COnnection to database can be done outside of functions once, 
# but do we need to proof check database connection works?
conn_string = "postgresql+psycopg2://postgres:pass@localhost:5432/weather_data"
#conn_string = "postgresql://postgres:pass@localhost/weather_data"  ##this one worked
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
    # 6     Denna har "w" som ger att skriva över. Ska vi skriva koden så den tittar om ett dokument med det namnet redan finns och gör nånting sen? 
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
    df = pd.DataFrame(create_harmony_dict) # Can I do this?************
    df.to_json(target_dir) #saves harmonized data

def sql_transfer():
    df = pd.DataFrame(create_harmony_dict) #this might need to be changed to ti.xcom_pull
    # to get updated data from harmonized.   Do we need intermediate def
    #from harmonized function that returns pandas weather_data dict?
    df.to_sql('weather_data', con=conn, if_exists='replace', index=False)
    conn = ps.connect(conn_string)
    conn.autocommit = True #do we need this?
    cursor = conn.cursor()
    weather1 = """select * from weather_data;"""
    cursor.execute(weather1)
    for i in cursor.fetchall():
        print(i)
    conn.close()





#####*****************************DAG*********************************************
with DAG("weather_dag", start_date=datetime(2022, 7, 14),
    schedule_interval="@hourly", catchup=False) as dag:

        save_rawdata = PythonOperator(
            task_id = "save_raw_data",
            python_callable=rawdata
        )

        temp_data = PythonOperator(
            task_id = "temp_dict",
            python_callable=parameterchoice('t')
        )
        
        pmean_data = PythonOperator(
            task_id = "pmean_dict",
            python_callable=parameterchoice('pmean')
        )
        
        msl_data = PythonOperator(
            task_id = "msl_dict",
            python_callable=parameterchoice('msl')
        )

        harmony_dict = PythonOperator(
            task_id = "harmony_dict",
            python_callable=save_harmony_data
        )

        to_sql = PythonOperator(
            task_id = "save_to_sql",
            python_callable=sql_transfer
        )


        [save_rawdata, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]
