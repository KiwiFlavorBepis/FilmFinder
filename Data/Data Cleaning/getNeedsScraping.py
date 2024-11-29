import pandas as pd
import numpy as np
from datetime import date
import os

#Set Paths
sourcePath = "cleanedData.csv"
noGenrePath = "noGenre.csv"

#Delete data needing scraping if it already exists
if os.path.exists(noGenrePath):
    # Delete the file
    os.remove(noGenrePath)
    print(f"{noGenrePath} has been deleted.")
else:
    print(f"{noGenrePath} does not exist.")

#load input data
data = pd.read_csv(sourcePath)

#get data without genre defined and create a new csv
noGenre = data[data['genres'].isnull() | (data['genres'] == '')]
noGenre.to_csv("noGenre.csv", index = False)