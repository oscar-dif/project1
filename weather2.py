import requests, os, configparser
import pandas as pd
import json


CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
data_dir = CURR_DIR_PATH + "/data/"
target_dir = data_dir + "/rawfiles/"

# 3 
WEATHER_URL = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json"
# GET /api/category/pmp3g/version/2/geotype/point/lon/16/lat/58/data.json

r = requests.get(WEATHER_URL)

# 5 
if r.status_code == 200: # If connection is successful (200: http ok)
    json_data = r.json() # Get result in json

# 5.1 
# create dictionary and run for loop
dict = json.loads(r.text)

#collect temperature data
temperature = []
for n in range(len(dict['timeSeries'])):
    for i in range(len(dict['timeSeries'][n]["parameters"])):
        if dict['timeSeries'][n]["parameters"][i]['name'] == "t":
            temp = dict['timeSeries'][n]["parameters"][i]['values'][0]
            temperature.append(temp)





#******************************************************************************************
#************************************collect date time data********************************
date = []
for i in range(len(dict['timeSeries'])):
    datedata = dict['timeSeries'][i]["validTime"]
    datedata = datedata[2:10] + " " + datedata[11:16]
    date.append(datedata)

#******************************************************************************************




# 6 
weather_data = {
                "temperature": json_data['timeSeries'][0]["parameters"][10]['values'][0],
                "air pressure": json_data['timeSeries'][0]["parameters"][11]['values'][0],
                "precipitation": json_data['timeSeries'][0]["parameters"][3]['values'][0],
                "date": json_data['timeSeries'][0]['validTime']
            }

weather_data = pd.json_normalize(weather_data)
print("this is a print statement with more stuff\n",weather_data)


#7 as part of raw data tast I would move this to the very beginning and collect and save all json data from api
df = pd.DataFrame(weather_data)
df.to_json(CURR_DIR_PATH + "/data/" + "data.json")
            

