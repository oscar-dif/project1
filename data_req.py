import requests, os, configparser
import pandas as pd
import matplotlib.pyplot as plt
import json
from sqlalchemy import create_engine
import psycopg2 as ps
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime, timedelta


######### paths #########
CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/raw_data/" + "data.json"
target_dir = CURR_DIR_PATH + "/target_data/" + "data.json"
######### /paths #########


######### postgres connection #########
#behöver en databas med namnet redan finnas? ja.
conn_string = "postgresql://postgres:lekrum123@localhost/weather_data"
db = create_engine(conn_string)
conn = db.connect()

######### /postgres connection #########


# 3 
######### URLs  #########
WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"
smhi = requests.get(WEATHER_URL)

if smhi.status_code == 200: # If connection is successful (200: http ok)
    json_data = smhi.json() # Get result in json
######### /URLs #########



######### Airflow and DAGS #########
def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'



def _training_model():
    return randint(1, 10)

with DAG("my_dag", start_date=datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=60), catchup=False) as dag: #https://www.astronomer.io/guides/scheduling-in-airflow/ # alternativt använd * * * * *

        training_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=_training_model
        )

        choose_best_model = BranchPythonOperator(
            task_id="choose_best_model",
            python_callable=_choose_best_model
        )

        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'"
        )

        inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate'"
        )

        [training_model_A] >> choose_best_model >> [accurate, inaccurate]









######### raw to harmonized #########


dict = json.loads(smhi.text)

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

######### /raw to harmonized #########


######### harmonized to staged #########
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

######### /harmonized to staged #########

# 6     Denna har "w" som ger att skriva över. Ska vi skriva koden så den tittar om ett dokument med det namnet redan finns och gör nånting sen? 
with open(data_dir, "w") as f:
    json.dump(json_data, f)



        