import requests, os, configparser
import pandas as pd
import matplotlib.pyplot as plt
import json

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/raw_data/" + "data.json"
target_dir = CURR_DIR_PATH + "/target_data/" + "data.json"

# 1 Get data
WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"
# GET /api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json

r = requests.get(WEATHER_URL)

# 2 Save json file with raw data
if r.status_code == 200: # If connection is successful (200: http ok)
    json_data = r.json() # Get result in json
with open(data_dir, "w") as f:
    json.dump(json_data, f)

# 3 function 
dict = json.loads(r.text)
def parameterchoice(choice):
    result = {} # empty list

    for n in range(len(dict['timeSeries'])):
        for i in range(len(dict['timeSeries'][n]["parameters"])):            
            if dict['timeSeries'][n]["parameters"][i]['name'] == choice:
                choice_value = dict['timeSeries'][n]["parameters"][i]['values'][0]
                
        result[f"{n}"] = choice_value
    return result

# print(parameterchoice('pmean'))
# print(parameterchoice('msl'))

weather_data = {
                "temperature": parameterchoice('t'),
                "air pressure": parameterchoice('pmean'),
                "precipitation": parameterchoice('msl')
            }
              
# ***************SHALL WE ADD DATES DATA TO THE DICTIONARY?*******************

# weather_data = pd.json_normalize(weather_data)

df = pd.DataFrame(weather_data)
df.to_json(target_dir)
