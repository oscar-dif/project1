import pandas as pd
import matplotlib.pyplot as plt
import os

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

# reading the database

data = pd.read_json(CURR_DIR_PATH + "/target_data/" + "data.json")
print(data)


# Line plot with day against tip
plt.plot(data['temperature'])
#plt.plot(data['precipitation'])

# Adding Title to the Plot
plt.title("Line Plot")

# Setting the X and Y labels
plt.xlabel('Hour')
plt.ylabel('temp C')

plt.show()
