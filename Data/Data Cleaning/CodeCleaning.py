import pandas as pd
import numpy as np
from datetime import date
import os

'''
    Functions:
'''
#Takes a function and data, prints changes from running the function, returns data
def analyzeCleaningFunction(func, data):
    beforeLength = len(data)

    data = func(data)

    afterLength = len(data)
    lengthDelta = beforeLength - afterLength

    print(f"Removed {lengthDelta} movies with function {func.__name__}. {afterLength} movies remaining.")
    return data

def removeDuplicates(data):
    data = data.drop_duplicates()
    return data

def removeUnreleased(data):
    data = data[data['status'] == "Released"]
    return data

def removeFutureRelease(data):
    today = pd.to_datetime(date.today())
    data['release_date'] = pd.to_datetime(data['release_date'])
    data = data[data['release_date'] < today]
    return data

def removePorn(data):
    data = data[data['adult'] == False]
    return data

def removeDuplicateID(data):
    data = data.drop_duplicates(subset=['id'], keep='first')
    return data

def removeUnusableColumns(data):
    badCols = ['status', 'backdrop_path', 'homepage', 'poster_path', 'adult']
    data = data.drop(columns = badCols)
    return data

def removeFakeDates(data):
    data = data[data['release_date'] >= pd.to_datetime('1865-01-01')]
    return data

def removeOldFilms(data):
    data = data[data['release_date'] > pd.to_datetime('1900-01-01')]
    return data

def removeNonFeatureFilms(data):
    data = data[data['runtime'] > 40]
    return data

def removeNoIMDBID(data):
    data = data[data['imdb_id'].notnull() & (data['imdb_id'] != '')]
    return data


#USER OPTIONS:
useFullDataSet = True
numberOfRows = 100000

#Set file location
file_location = "TMDB_movie_dataset_v11.csv"
cleaned_destination = "cleaned_data.csv"

if useFullDataSet:
    data = pd.read_csv(file_location)
else: 
    data = pd.read_csv(file_location, nrows=numberOfRows)

#Delete cleaned data if it exists in this directory
if os.path.exists(cleaned_destination):
    # Delete the file
    os.remove(cleaned_destination)
    print(f"{cleaned_destination} has been deleted.")
else:
    print(f"{cleaned_destination} does not exist.")


#Perform functions on the data set:

data = analyzeCleaningFunction(removeDuplicates, data)

data = analyzeCleaningFunction(removeUnreleased, data)

data = analyzeCleaningFunction(removeFutureRelease, data)

data = analyzeCleaningFunction(removePorn, data)

data = analyzeCleaningFunction(removeDuplicateID, data)

data = removeUnusableColumns(data)

data = analyzeCleaningFunction(removeFakeDates, data)

data = analyzeCleaningFunction(removeOldFilms, data)

data = analyzeCleaningFunction(removeNonFeatureFilms, data)

data = analyzeCleaningFunction(removeNoIMDBID, data)

#save the data to a new CSV
data.to_csv("cleanedData.csv", index = False)

#Data Analysis
numNoID = data['id'].isnull().sum() + (data['id'] == '').sum()
numNoTitle = data['title'].isnull().sum() + (data['title'] == '').sum()
numNoIMDBID = data['imdb_id'].isnull().sum() + (data['imdb_id'] == '').sum()
numNoGenre = data['genres'].isnull().sum() + (data['genres'] == '').sum()
numNoKeyWords = data['keywords'].isnull().sum() + (data['keywords'] == '').sum()

# Print the results
print(f"Number of missing 'id' or empty: {numNoID}")
print(f"Number of missing 'title' or empty: {numNoTitle}")
print(f"Number of missing 'imdb_id' or empty: {numNoIMDBID}")
print(f"Number of missing 'genres' or empty: {numNoGenre}")
print(f"Number of missing 'keywords' or empty: {numNoKeyWords}")


