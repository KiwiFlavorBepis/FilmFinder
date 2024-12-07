import multiprocessing
import warnings
import pandas as pd
from bs4 import BeautifulSoup
import re
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import time
import random

# an attempt at multiprocess scraping using the code from imdb_scraper.py

def parse_runtime_to_minutes(runtime_text):

    hours_match = re.search(r'(\d+)hours?', runtime_text)
    minutes_match = re.search(r'(\d+)minutes?', runtime_text)

    total_minutes = 0
    if hours_match:
        total_minutes += int(hours_match.group(1)) * 60
    if minutes_match:
        total_minutes += int(minutes_match.group(1))

    return total_minutes if total_minutes > 0 else None


def create_retry_session(retries=5, backoff_factor=0.5, status_forcelist=(500, 502, 504, 429, 403), session=None):

    session = session or requests.Session()

    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=False
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


def scrape_imdb_id(imdb_id, **kwargs):
    column = kwargs.get('column', 'all')

    print(f"\nScraping for {imdb_id} (Columns: {column})")

    session = create_retry_session()

    time.sleep(random.uniform(1, 5))

    def safe_request(url, session=session):
        """
        Safely make a request with error handling and logging
        """

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win 64 ; x64) Apple WeKit /537.36(KHTML , like Gecko) Chrome/80.0.3987.162 Safari/537.36'
        }

        try:
            response = session.get(url, headers=headers, timeout=10)

            if response.status_code != 200:
                print(f"Warning: Status code {response.status_code} for {url}")
                return None

            return response
        except requests.exceptions.RequestException as e:
            print(f"Request error for {url}: {e}")
            return None

    urls = {
        'main': f"https://www.imdb.com/title/{imdb_id}/",
        'summary': f"https://www.imdb.com/title/{imdb_id}/plotsummary/",
        'tagline': f"https://www.imdb.com/title/{imdb_id}/taglines/",
        'keywords': f"https://www.imdb.com/title/{imdb_id}/keywords/"
    }

    responses = {}
    if column == 'all':
        for key, url in urls.items():
            response = safe_request(url)
            if response is None:
                print(f"Failed to retrieve {key} page for {imdb_id}")
                return None
            responses[key] = response
    else:
        match column:
            case 'overview':
                response = safe_request(urls['summary'])
            case 'tagline':
                response = safe_request(urls['tagline'])
            case 'keywords':
                response = safe_request(urls['keywords'])
            case _:
                response = safe_request(urls['main'])
        if not response:
            print(f"Failed to retrieve {column} page for {imdb_id}")
            return None
        responses[column] = response

    movie_data = {}
    print(f"\nParsing scraped data for {imdb_id}")
    try:
        if column == 'all' or column != 'overview' or column != 'tagline' or column != 'keywords':
            soup = BeautifulSoup(responses['main'].text, 'html.parser')
            # Scraping genres
            chip_list_scrollers = soup.find_all('div', class_='ipc-chip-list__scroller')
            if chip_list_scrollers:
                genres_scroller = chip_list_scrollers[0]
                genres = [
                    genre.find('span', class_='ipc-chip__text').get_text(strip=True)
                    for genre in genres_scroller.find_all('a', class_='ipc-chip')
                ]
                if genres:
                    movie_data['genres'] = ', '.join(genres)
                    print(f"    New Genres: {', '.join(genres)}")
                else:
                    print("    Genres not found")
            # Scraping for movie runtime
            runtime_element = soup.find('li', attrs={'data-testid': 'title-techspec_runtime'})
            if runtime_element:
                runtime_text = runtime_element.find('div', class_='ipc-metadata-list-item__content-container').get_text(
                    strip=True)
                if runtime_text:
                    runtime_minutes = parse_runtime_to_minutes(runtime_text)
                    if runtime_minutes:
                        movie_data['runtime'] = runtime_minutes
                        print(f"    New Runtime: {runtime_minutes}")
            else:
                print("    Runtime: Not found")
            # Scraping languages
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
                    print(f"    New Languages: {languages_str}")
                else:
                    print("    Languages not found")
            else:
                print("    Languages element not found")
            # Scraping production companies
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
                    print(f"    New Production Companies: {companies_str}")
                else:
                    print("    Production companies links not found")
            else:
                print("    Production companies element not found")
            # Scraping production countries
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
                    print(f"    New Production Countries: {countries_str}")
                else:
                    print("    Production countries links not found")
            else:
                print("    Production countries element not found")
    except Exception as e:
        print(f"    Couldn't parse {imdb_id} (Column: {column}): {urls['main']} : {e}")
        return None

    try:
        if column == 'overview':
            soup_summary = BeautifulSoup(responses['summary'].text, 'html.parser')
            # Scraping plot summary
            plot_li = soup_summary.find('li', id=lambda x: x and x.startswith('po'))

            if plot_li:
                plot_div = plot_li.find('div', class_='ipc-html-content-inner-div', recursive=True)

                if plot_div:
                    overview_text = plot_div.get_text(strip=True)
                    movie_data['overview'] = overview_text
                    print(f"    New Plot Overview: {overview_text}")
                else:
                    print("    Plot overview not found")
            else:
                print("    Plot overview element not found")
    except Exception as e:
        print(f"    Couldn't parse {imdb_id} (Column: {column}): {urls['summary']} : {e}")
        return None

    try:
        if column == 'tagline':
            soup_tagline = BeautifulSoup(responses['tagline'].text, 'html.parser')
            # Scraping plot tagline
            tagline_ul = soup_tagline.find('ul',
                                           class_='ipc-metadata-list ipc-metadata-list--dividers-between sc-bda8bbe6-0 jxZlgE meta-data-list-full ipc-metadata-list--base')
            if tagline_ul:
                first_tagline_li = tagline_ul.find('li', class_='ipc-metadata-list__item')
                if first_tagline_li:
                    tagline_div = first_tagline_li.find('div', class_='ipc-html-content-inner-div')
                    first_tagline_text = tagline_div.get_text(strip=True) if tagline_div else None
                    movie_data['tagline'] = first_tagline_text
                    print(f"    New Plot Tagline: {first_tagline_text}")
                else:
                    print("    Plot tagline not found")
            else:
                print("    Plot tagline element not found")
    except Exception as e:
        print(f"    Couldn't parse {imdb_id} (Column: {column}): {urls['tagline']} : {e}")
        return None

    try:
        if column == 'keywords':
            soup_keywords = BeautifulSoup(responses['keywords'].text, 'html.parser')
            # Scraping keywords
            keywords_ul = soup_keywords.find('ul', class_=lambda x: x and 'ipc-metadata-list' in x)
            keywords = []
            keywords_str = None
            if keywords_ul:
                keyword_items = keywords_ul.find_all('li', class_=lambda x: x and 'ipc-metadata-list-summary-item' in x,
                                                     limit=15)

                for keyword_li in keyword_items:
                    keyword_div = keyword_li.find('a', class_=lambda x: x and 'ipc-metadata-list-summary-item__t' in x)
                    if keyword_div:
                        keyword = keyword_div.get_text(strip=True)
                        keywords.append(keyword)
                if keywords:
                    keywords_str = ', '.join(keywords)
                    movie_data['keywords'] = keywords_str
                    print(f"    New Plot Keywords: {keywords_str}")
                else:
                    print("    Plot keywords not found")
            else:
                print("    Plot keyword element not found")
    except Exception as e:
        print(f"    Couldn't parse {imdb_id} (Column: {column}): {urls['keywords']} : {e}")
        return None

    print()
    return movie_data

def print_movie_data(index, movie, **kwargs):
    verbose = kwargs.get('verbose', False)

    print(f"Index: {index}")
    print(f"Title: {movie.title}")
    print(f"IMDb ID: {movie.imdb_id}\n")
    if verbose:
        print(f"Runtime: {movie.runtime}")
        print(f"Genres: {movie.genres}")
        print(f"Languages: {movie.spoken_languages}")
        print(f"Overview: {movie.overview[:100]}..." if not pd.isnull(movie.overview) else "Current Overview: None")
        print(f"Tagline: {movie.tagline}" if not pd.isnull(movie.tagline) else "Current Tagline: None")
        print(f"Production Companies: {movie.production_companies}" if not pd.isnull(
            movie.production_companies) else "Current Production Companies: None")
        print(f"Production Countries: {movie.production_countries}" if not pd.isnull(
            movie.production_countries) else "Current Production Countries: None")
        print(f"Keywords: {movie.keywords}" if not pd.isnull(movie.keywords) else "Current Keywords: None")

def process_movie(index, movie):
    imdb_id = movie.imdb_id

    print_movie_data(index, movie)

    if pd.isnull(movie.overview):
        movie_data = scrape_imdb_id(imdb_id, column='overview')
        if movie_data:
            movie['overview'] = movie_data['overview']
    if pd.isnull(movie.tagline):
        movie_data = scrape_imdb_id(imdb_id, column='tagline')
        if movie_data:
            movie['tagline'] = movie_data['tagline']
    if pd.isnull(movie.keywords):
        movie_data = scrape_imdb_id(imdb_id, column='keywords')
        if movie_data:
            movie['keywords'] = movie_data['keywords']

    if movie.isnull().any():
        movie_data = scrape_imdb_id(imdb_id)
        if movie_data:
            for column, value in movie.items():
                if pd.isnull(value) and column in movie_data:
                    print(f"Swapped {movie_data[column]} into {movie[column]}")
                    movie[column] = movie_data[column]
        else:
            print(f"Skipping movie {imdb_id} due to scraping failure")

    return index, movie

def process_chunk(chunk):
    for index, movie in chunk.iterrows():
        result_index, result_movie = process_movie(index, movie)
        result_movie = result_movie.drop(['Unnamed: 0'])
        movie = result_movie

    return chunk

def update_movie_dataset(input_file, **kwargs):

    df = pd.read_csv(input_file)

    start_line = kwargs.get('start_line', 0)
    end_line = kwargs.get('end_line', len(df))
    processes = kwargs.get('processes', 4)

    end_line += 1
    df_subset = df.iloc[start_line : end_line]

    if processes == multiprocessing.cpu_count():
        warnings.warn(f"Number of processes ({processes}) exceeds number of CPU cores ({multiprocessing.cpu_count()})!")

    pool = multiprocessing.Pool(processes)
    chunk_size = int(df_subset.shape[0] / processes)
    chunks = [df_subset.iloc[i:i + chunk_size] for i in range(0, df_subset.shape[0], chunk_size)]

    print("Starting Scrape")
    print(f"Total movies in dataset range: {len(df_subset)}")
    print(f"Processing from movie {start_line} to movie {end_line} in {len(chunks)} chunks of {chunk_size}")
    print("-" * 50)

    outputs_async = pool.map_async(process_chunk, chunks)
    outputs = outputs_async.get()
    pool.close()

    return outputs

if __name__ == "__main__":

    input_file = '../Datasets/clean.csv'
    start_line = 100
    end_line = 200 #inclusive
    start = time.time()
    results = update_movie_dataset(input_file, start_line=start_line, end_line=end_line, processes=15)
    output = pd.concat(results).drop('Unnamed: 0', axis=1)
    output.to_csv(f'../Datasets/scraped_{start_line}_{end_line}.csv')
    print(f"Took {time.time() - start} seconds to process {end_line - start_line + 1} movies")