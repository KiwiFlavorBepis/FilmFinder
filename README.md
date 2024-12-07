# FilmFinder
CSDS 395 Senior Project

This repository contains the code for cleaning the initial TMDb dataset and performing web scraping using the IMDb ID's in the dataset.

Steps to use the files in FilmFinder:

1. Download the file linked in our final report labeled [Original Dataset from TMDB](https://drive.google.com/file/d/194FQsDnBn2zF3ZzUC6wRUIRBf6xgpkdp/view?usp=sharing). Add the TMDB dataset to the folder "Datasets" within the "Data" directory in this repository. Then, in Data/Data Cleaning, run the file Data_Cleaning.ipynb (Data/Data Cleaning/Data_Cleaning.ipynb). This will provide a demonstration of how initial data cleaning was carried out.

2. Download the file linked in the final report labeled [Cleaned](https://drive.google.com/file/d/1KONkZi5b0oGQFhCHCY61BcRf7mGPXLv7/view). This file should be called "modern_feature_films.csv". Add the csv file to the root directory of this repository. Go to IMDb_scraper.py in the dirctory Data/Scraper and scroll to the bottom of the script. Ensure that the variable titled input_file matches the name "modern_feature_films.csv".
The method update_movie_dataset() takes an interval of movies by line index in the .csv file for which to scrape. The variable saveInterval will be the regular interval at which the script saves the scraped movies to a new csv file. For example, if start = 1, end = 100, and saveInterval = 10, the script will save movies 1-10 to a csv, then 2-20, etc. modern_feature_films.csv contains 314210 lines (movies). Since scraping the entire .csv file takes too long, set a start, end, and saveInterval short enough to view the results. (Note: the variable "start" must be at least 1).
Afterwards, you will see a number of .csv files for the scraped data in the root directory. The titles of the .csv files will indicate which scraped movies are within. We did not have an official script to merge all the .csv files, but this can be easily done in Python.
To run the final web app in the separate [FilmFinderWebApp](https://github.com/JoshuaMeyer1/FilmFinderWebApp) GitHub repository, use the [final cleaned and scraped dataset](https://drive.google.com/file/d/1-DYeF2MsXQ_hgA5yf3SBO93U6ZsZEkc_/view?usp=sharing) and follow the instructions in the othe repository.
