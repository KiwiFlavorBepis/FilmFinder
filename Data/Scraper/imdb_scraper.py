import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import time
import random

def parse_runtime_to_minutes(runtime_text):
    # converting runtime to correct format
    hours_match = re.search(r'(\d+)hours?', runtime_text)
    minutes_match = re.search(r'(\d+)minutes?', runtime_text)
    
    total_minutes = 0
    if hours_match:
        total_minutes += int(hours_match.group(1)) * 60
    if minutes_match:
        total_minutes += int(minutes_match.group(1))
    
    return total_minutes if total_minutes > 0 else None

def create_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504, 429, 403),
    session=None
):

    # create a requests session with retry capabilities
    session = session or requests.Session()
    
    # Configure retry strategy
    retry = Retry(

        # create a requests session with retry capabilities
        total=retries,
        read=retries,

        connect=retries,
        backoff_factor=backoff_factor,

        # status errors for which you can retry
        status_forcelist=status_forcelist,
        raise_on_status=False 
    )
    
    # adapter with retry specs
    adapter = HTTPAdapter(max_retries=retry)
    
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

def scrape_imdb_id(imdb_id):
    # allowing retries
    session = create_retry_session()

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/80.0.3987.162 Safari/537.36'
    }

    # add random timer so that imdb does not block ip from syrpassing rate limit
    time.sleep(random.uniform(1, 3))

    def safe_request(url, session=session, headers=header):
        """
        Safely make a request with error handling and logging
        """
        try:
            response = session.get(url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                print(f"Warning: Status code {response.status_code} for {url}")
                return None
            
            return response
        except requests.exceptions.RequestException as e:
            print(f"Request error for {url}: {e}")
            return None

    # URLs to be scraped (keywords and main movie page)
    urls = {
        'main': f"https://www.imdb.com/title/{imdb_id}/",
        'keywords': f"https://www.imdb.com/title/{imdb_id}/keywords/"
    }

    # getting html of each web page
    responses = {}
    for key, url in urls.items():
        response = safe_request(url)
        if response is None:
            print(f"Failed to retrieve {key} page for {imdb_id}")
            return None
        responses[key] = response

    movie_data = {}
    
    try:
        soup = BeautifulSoup(responses['main'].text, 'html.parser')
        soup_keywords = BeautifulSoup(responses['keywords'].text, 'html.parser')

        # scraping genres
        genre_element = soup.find_all('div', class_='ipc-chip-list__scroller')
        if genre_element:
            genres_scroller = genre_element[0]
            genres = [
                genre.find('span', class_='ipc-chip__text').get_text(strip=True) 
                for genre in genres_scroller.find_all('a', class_='ipc-chip')
            ]
            if genres:
                movie_data['genres'] = ', '.join(genres)
                print(f"New Genres: {', '.join(genres)}")
            else:
                print("Genres not found")

        # scraping for movie runtime
        runtime_element = soup.find('li', attrs={'data-testid': 'title-techspec_runtime'})
        if runtime_element:
            runtime_text = runtime_element.find('div', class_='ipc-metadata-list-item__content-container').get_text(strip=True)
            if runtime_text:
                runtime_minutes = parse_runtime_to_minutes(runtime_text)
                if runtime_minutes:
                    movie_data['runtime'] = runtime_minutes
                    print(f"New Runtime: {runtime_minutes}")
        else:
            print("Runtime: Not found")

        # scraping languages
        languages_element = soup.find('li', {'data-testid': 'title-details-languages'})
        languages_list = []
        
        if languages_element:
            language_links = languages_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
            
            if language_links:
                for language_link in language_links:
                    language = language_link.get_text(strip=True)
                    languages_list.append(language)
                
                languages_str = ', '.join(languages_list)
                movie_data['spoken_languages'] = languages_str
                print(f"New Languages: {languages_str}")
            else:
                print("Languages not found")
        else:
            print("Languages element not found")

        # scraping production companies
        companies_element = soup.find('li', {'data-testid': 'title-details-companies'})
        companies_list = []
        
        if companies_element:
            company_links = companies_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
            
            if company_links:
                for company_link in company_links:
                    company = company_link.get_text(strip=True)
                    companies_list.append(company)
                
                companies_str = ', '.join(companies_list)
                movie_data['production_companies'] = companies_str
                print(f"New Production Companies: {companies_str}")
            else:
                print("Production companies links not found")
        else:
            print("Production companies element not found")

        # scraping production countries
        countries_element = soup.find('li', {'data-testid': 'title-details-origin'})
        countries_list = []
        
        if countries_element:
            country_links = countries_element.find_all('a', class_='ipc-metadata-list-item__list-content-item')
            
            if country_links:
                for country_link in country_links:
                    country = country_link.get_text(strip=True)
                    countries_list.append(country)
                
                countries_str = ', '.join(countries_list)
                movie_data['production_countries'] = countries_str
                print(f"New Production Countries: {countries_str}")
            else:
                print("Production countries links not found")
        else:
            print("Production countries element not found")



        # Scraping keywords
        keywords_ul = soup_keywords.find('ul', class_=lambda x: x and 'ipc-metadata-list' in x)

        keywords = []
        keywords_str = None
        if keywords_ul:
            keyword_items = keywords_ul.find_all('li', class_=lambda x: x and 'ipc-metadata-list-summary-item' in x, limit=15)
            
            for keyword_li in keyword_items:
                keyword_div = keyword_li.find('a', class_=lambda x: x and 'ipc-metadata-list-summary-item__t' in x)
                if keyword_div:
                    keyword = keyword_div.get_text(strip=True)
                    keywords.append(keyword)
            if keywords:
                keywords_str = ', '.join(keywords)
                movie_data['keywords'] = keywords_str
                print(f"New Plot Keywords: {keywords_str}")
            else:
                print("Plot keywords not found")
        else:
            print("Plot keyword element not found")

        return movie_data
        
    except Exception as e:
        print(f"Error for movie {imdb_id}: {str(e)}")
        return None

def update_movie_dataset(input_file, saveInterval, start_line=None, end_line=None):

    df = pd.read_csv(input_file)
    
    if start_line is None:
        start_line = 0
    else:
        start_line = start_line - 1
    
    if end_line is None:
        end_line = len(df)
    else:
        end_line = end_line - 1
    
    print("\n Scraping start")
    print(f"Total movies in dataset: {end_line - start_line + 1}")
    print(f"Processing from movie {start_line + 1} to movie {end_line + 1}")
    print("\n")
    
    counter = start_line
    while counter <= end_line:
        df_subset = df.iloc[counter: (counter + saveInterval)]
        for index, row in df_subset.iterrows():
            imdb_id = row['imdb_id']
            
            print(f"\nMovie {index + 1}:")
            print(f"Title: {row['title']}")
            print(f"IMDb ID: {imdb_id}")
            print(f"Runtime: {row['runtime']}")
            print(f"Genres: {row['genres']}")
            print(f"Languages: {row['spoken_languages']}")
            print(f"Production Companies: {row['production_companies']}" if not pd.isnull(row['production_companies']) else "Current Production Companies: None")
            print(f"Production Countries: {row['production_countries']}" if not pd.isnull(row['production_countries']) else "Current Production Countries: None")
            print(f"Keywords: {row['keywords']}" if not pd.isnull(row['keywords']) else "Current Keywords: None")
            
            if (pd.isnull(row['genres']) or pd.isnull(row['runtime']) or 
                pd.isnull(row['spoken_languages']) or pd.isnull(row['production_companies']) or
                pd.isnull(row['production_countries']) or pd.isnull(row['keywords'])):

                print()
                print(f"WEB SCRAPED DATA FOR {imdb_id} ({row['title'].upper()}):")
                
                movie_data = scrape_imdb_id(imdb_id)

                # Skip invalid URLs or IMDb ID's
                if movie_data is None:
                    print(f"Skipping movie {imdb_id} due to scraping failure")
                    counter += 1
                    if (counter > end_line):
                        break
                    continue
                
                try: 
                    if pd.isnull(row['genres']) and 'genres' in movie_data:
                        df.at[index, 'genres'] = movie_data['genres']
                    
                    if pd.isnull(row['runtime']) and 'runtime' in movie_data:
                        df.at[index, 'runtime'] = movie_data['runtime']
                    
                    if pd.isnull(row['spoken_languages']) and 'spoken_languages' in movie_data:
                        df.at[index, 'spoken_languages'] = movie_data['spoken_languages']
                    
                    if pd.isnull(row['production_companies']) and 'production_companies' in movie_data:
                        df.at[index, 'production_companies'] = movie_data['production_companies']
                    
                    if pd.isnull(row['production_countries']) and 'production_countries' in movie_data:
                        df.at[index, 'production_countries'] = movie_data['production_countries']
                    
                    if pd.isnull(row['keywords']) and 'keywords' in movie_data:
                        df.at[index, 'keywords'] = movie_data['keywords']
                except Exception as e:
                    print(f"Error: {e}")
            
            counter += 1
            if (counter > end_line):
                break
            
            print("-" * 50)

        sectionStart = counter - saveInterval
        name = "scraped_" + str(sectionStart + 1) + "_" + str(counter + 1) + ".csv"
        df_subset.to_csv(name, index=False)
        numScrapesLeft = saveInterval

        df_subset = df.iloc[counter:(counter + saveInterval - 1)]
        print("Scraping save")
    print("Scraping finished")


if __name__ == "__main__":
    input_file = 'modern_feature_films.csv'
    
    start = 15000
    end = 15100
    saveInterval = 1000
    
    update_movie_dataset(input_file, saveInterval, start, end)