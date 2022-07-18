from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator


from datetime import datetime
import requests, os
import pandas as pd
import matplotlib.pyplot as plt
import json
from sqlalchemy import *
import psycopg2 as ps

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/raw_data/" + "data.json"
target_dir = CURR_DIR_PATH + "/target_data/" + "data.json"

conn_string = "postgresql://postgres:pass@127.0.0.1/weather_data"
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

def date_collect():
    r = request()
    dict = json.loads(r.text)
    result = {}
    for n in range(len(dict['timeSeries'])):
        datedata = dict['timeSeries'][n]["validTime"]
        datedata = datedata[2:10] + " " + datedata[11:16]  #comment out to keep the original formatting
        result[f"{n}"] = datedata   
    return result
    
def create_harmony_dict(): #creates dict from selected data
    weather_data = {
                    "date": date_collect(),
                    "temperature": parameterchoice('t'),
                    "air pressure": parameterchoice('pmean'),
                    "precipitation": parameterchoice('msl')
                }
    return weather_data

def line_plot():
    data = pd.read_json(CURR_DIR_PATH + "/target_data/" + "data.json")
    fig, ax = plt.subplots() 
    plt.title("Line Plot")
    # Line plot with day against tip
    ax.plot(data['temperature'], color = 'green')
    ax.tick_params(axis='y', labelcolor='green')
    ax.set_xlabel('Hourly')
    ax.set_ylabel('Temperature')    
    #adding second y axis
    ax2 = ax.twinx()
    ax2.set_ylabel('Precipitation')
    ax2.plot(data['precipitation'], color = 'blue')
    ax2.tick_params(axis='y', labelcolor='blue')
    plt.show()

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
with DAG("weather_dag", start_date=datetime(2022, 7, 14),
    schedule_interval="@hourly", catchup=False) as dag:

        save_rawdata = PythonOperator(
            task_id = "save_rawdata",
            python_callable=rawdata
        )

        create_harmony_dict = PythonOperator(
            task_id = "create_harmony_dict",
            python_callable=create_harmony_dict
        )
        

        line_plot = PythonOperator(
            task_id = "line_plot",
            python_callable=line_plot
        )

        harmony_dict = PythonOperator(
            task_id = "harmony_dict",
            python_callable=save_harmony_data
        )

        to_sql = PythonOperator(
            task_id = "save_to_sql",
            python_callable=sql_transfer
        )


        [save_rawdata, create_harmony_dict] >> line_plot >> harmony_dict >> to_sql
