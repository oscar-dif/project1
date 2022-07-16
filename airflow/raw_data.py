import requests, os, configparser
import pandas
import matplotlib.pyplot
import json
from sqlalchemy import create_engine
import psycopg2 as ps
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# # # # # # # # # # # # # # # # # # # # # # # # # # # # 
			READ THIS!
IF YOU WANT TO TEST THIS FILE YOU NEED TO HAVE A FOLDER,
			"raw_data", 
	     IN THE SAME PLACE AS THIS FILE.

# # # # # # # # # # # # # # # # # # # # # # # # # # # # 





CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/raw_data/" + "data.json"
# target_dir = CURR_DIR_PATH + "/target_data/" + "data.json"



WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"
smhi = requests.get(WEATHER_URL)



def raw_to_harmonized():
    smhi = requests.get(WEATHER_URL)

    if smhi.status_code == 200: # If connection is successful (200: http ok)
        json_data = smhi.json() # Get result in json
    
    with open(data_dir, "w") as f:
        return json.dump(json_data, f)


#### 2
with DAG("raw_data", start_date=datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=1), catchup=False) as dag: 

        raw_to_harmonized = PythonOperator(
            task_id="raw_to_harmonized",
            python_callable=raw_to_harmonized
        )

        [raw_to_harmonized]