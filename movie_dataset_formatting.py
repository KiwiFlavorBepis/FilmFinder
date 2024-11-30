from bs4 import BeautifulSoup
import requests, json, lxml
import coverage
import os
#Import the code coverage class so data can be run when this class is called
cov = coverage.Coverage()
cov.start()

def clean_data():
    # cleaning data formatting
    with open('modern_feature_films.csv', 'r', encoding='utf-8') as file:
        content = file.read()

    cleaned_content = content.replace('\u2028', '\n').replace('\u2029', '\n')

    with open('modern_feature_films.csv', 'w', encoding='utf-8') as cleaned_file:
        cleaned_file.write(cleaned_content)

clean_data()